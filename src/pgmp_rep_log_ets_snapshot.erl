%% Copyright (c) 2023 Peter Morgan <peter.james.morgan@gmail.com>
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.


-module(pgmp_rep_log_ets_snapshot).


-export([callback_mode/0]).
-export([handle_event/4]).
-import(pgmp_rep_log_ets_common, [insert_new/5]).
-import(pgmp_rep_log_ets_common, [metadata/4]).
-import(pgmp_rep_log_ets_common, [new_table/4]).
-import(pgmp_statem, [nei/1]).
-include_lib("kernel/include/logger.hrl").


callback_mode() ->
    handle_event_function.


handle_event({call, _}, _, _, _) ->
    {keep_state_and_data, postpone};

handle_event(internal,
             {response, #{reply := [{command_complete, 'begin'}]}},
             query,
             Data) ->
    {next_state, unready, Data};

handle_event(internal,
             {response, #{reply := [{command_complete, commit}]}},
             _,
             #{stream := Stream} = Data) ->
    {next_state,
     ready,
     maps:without([stream], Data),
     [pop_callback_module,
      {reply, Stream, ok}]};

handle_event(internal,
             {response,
              #{label := sync_publication_tables = Label,
                reply := [{parse_complete, []}]}},
             parse,
             #{config := #{publication := Publication}} = Data) ->
    {next_state,
     unready,
     Data,
     nei({bind, #{label => Label, args => [Publication]}})};

handle_event(internal,
             {response,
              #{label := sync_publication_tables = Label,
                reply := [{bind_complete, []}]}},
             bind,
             Data) ->
    {next_state,
     unready,
     Data,
     nei({execute, #{label => Label}})};

handle_event(internal,
             {response, #{label := sync_publication_tables,
                          reply := [{command_complete, {select, 0}}]}},
             execute,
             Data) ->
    %% There are no publication tables to sync, initiate streaming
    %% replication.
    %%
    {next_state, unready, Data, nei(commit)};

handle_event(internal,
             {response, #{label := sync_publication_tables,
                          reply := [{row_description, Columns} | T]}},
             execute,
             Data) ->
    {command_complete, {select, _}} = lists:last(T),
    {next_state,
     unready,
     Data,
     nei({fetch,
          lists:map(
            fun
                ({data_row, Values}) ->
                    maps:from_list(lists:zip(Columns, Values))
            end,
            lists:droplast(T))})};

handle_event(internal,
             {fetch, []},
             unready,
             _) ->
    {keep_state_and_data, nei(commit)};

handle_event(internal,
             {fetch, _} = Label,
             unready,
             _) ->
    {keep_state_and_data,
     nei({parse,
          #{label => Label,
            sql => <<"select i.indkey from pg_catalog.pg_index i"
                     ", pg_catalog.pg_namespace n"
                     ", pg_catalog.pg_class c"
                     " where "
                     "i.indisprimary"
                     " and "
                     "i.indrelid = c.oid"
                     " and "
                     "c.relnamespace = n.oid"
                     " and "
                     "n.nspname = $1"
                     " and "
                     "c.relname = $2">>}})};

handle_event(internal,
             {response,
              #{label := {fetch, [#{<<"schemaname">> := Schema, <<"tablename">> := Table} | _]} = Label,
                reply := [{parse_complete, []}]}},
             parse,
             Data) ->
    {next_state,
     unready,
     Data,
     nei({bind, #{label => Label, args => [Schema, Table]}})};

handle_event(internal,
             {response,
              #{label := {fetch, _} = Label,
                reply := [{bind_complete, []}]}},
             bind,
             Data) ->
    {next_state,
     unready,
     Data,
     nei({execute, #{label => Label}})};

handle_event(
  internal,
  {response,
   #{label := {fetch, [#{<<"tablename">> := _}  = Publication | T]},
     reply := [{command_complete, {select, 0}}]}},
  execute,
  Data) ->
    ?LOG_WARNING(
       #{publication => Publication,
         reason => "no primary key found"}),
    {next_state, unready, Data, nei({fetch, T})};

handle_event(
  internal,
  {response,
   #{label := {fetch,
               [#{<<"schemaname">> := Namespace,
                  <<"tablename">> := Name} = Info | T]},
     reply := [{row_description, [<<"indkey">>]},
               {data_row, [Key]},
               {command_complete, {select, 1}}]}},
  execute,
  #{metadata := Metadata,
    config := #{publication := Publication}} = Data) ->
    {next_state,
     unready,
     Data#{metadata := metadata(
                         {Namespace, Name},
                         keys,
                         Key,
                         metadata(
                           {Namespace, Name},
                           table,
                           new_table(Publication, Namespace, Name, Key),
                           Metadata))},
     [nei({parse,
           #{label => {table, #{namespace => Namespace, name => Name}},
             sql => pub_fetch_sql(Info)}}),
      nei({fetch, T})]};

handle_event(internal,
             {response,
              #{label := {table, _} = Label,
                reply := [{parse_complete, []}]}},
             parse,
             Data) ->
    {next_state, unready, Data, nei({bind, #{label => Label}})};

handle_event(internal,
             {response,
              #{label := {table, _} = Label,
                reply := [{bind_complete, []}]}},
             bind,
             Data) ->
    {next_state, unready, Data, nei({describe, #{type => $P, label => Label}})};


handle_event(internal,
             {response,
              #{label := {table,
                          #{namespace := Namespace, name := Name}} = Label,
                reply := [{row_description, Columns}]}},
             describe,
             #{metadata := Metadata} = Data) ->
    {next_state,
     unready,
     Data#{metadata := metadata(
                         {Namespace, Name},
                         columns,
                         [FieldName || #{field_name := FieldName} <- Columns],
                         metadata(
                           {Namespace, Name},
                           oids,
                           [OID || #{type_oid := OID} <- Columns],
                           Metadata))},
     nei({execute,
          #{label => Label,
            max_rows => pgmp_config:replication(
                          logical,
                          max_rows)}})};


handle_event(internal,
             {response, #{label := {table, _},
                          reply := [{command_complete, {select, 0}}]}},
             execute,
             Data) ->
    {next_state, unready, Data};

handle_event(internal,
             {response,
              #{label := {table,
                          #{namespace := Namespace,
                            name := Name}} = Label,
                          reply := [{row_description, Columns} | T]}},
             execute,
             #{metadata := Metadata,
               config := #{publication := Publication}} = Data) ->

    #{{Namespace, Name} := #{keys := Keys}} = Metadata,

    ok = insert_new(
           Publication,
           Namespace,
           Name,
           [list_to_tuple(Values) || {data_row, Values} <- lists:droplast(T)],
           Keys),

    case lists:last(T) of
        {command_complete, {select, _}} ->
            {next_state,
             unready,
             Data#{metadata := metadata({Namespace, Name}, columns, Columns, Metadata)}};

        {portal_suspended, _} ->
            {next_state,
             unready,
             Data,
             nei({execute,
                  #{label => Label,
                    max_rows => pgmp_config:replication(
                                  logical,
                                  max_rows)}})}
    end;

handle_event(internal,
             {response, #{reply := [{command_complete, set}]}},
             query,
             Data) ->
    {next_state, unready, Data};

handle_event(internal,
             sync_publication_tables = Label,
             _,
             _) ->
    {keep_state_and_data,
     nei({parse,
          #{label => Label,
            sql => <<"select * from pg_catalog.pg_publication_tables "
                     "where pubname = $1">>}})};

handle_event(internal, begin_transaction = Label, _, _) ->
    {keep_state_and_data,
     nei({query,
          #{label => Label,
            sql => <<"begin isolation level repeatable read">>}})};

handle_event(internal, commit = Label, _, _) ->
    {keep_state_and_data,
     nei({query, #{label => Label, sql => <<"commit">>}})};

handle_event(internal, {set_transaction_snapshot = Label, Id}, _, _) ->
    {keep_state_and_data,
     nei({query,
          #{label => Label,
            sql => io_lib:format(
                     "SET TRANSACTION SNAPSHOT '~s'",
                     [Id])}})};

handle_event(internal,
             {Action, Arg},
             unready,
             #{requests := Requests} = Data)
  when Action == query;
       Action == parse;
       Action == bind;
       Action == describe;
       Action == execute ->
    {next_state,
     Action,
     Data#{requests := pgmp_connection:Action(
                         Arg#{requests => Requests})}};

handle_event(internal, {Action, _}, _, _)
  when Action == query;
       Action == parse;
       Action == bind;
       Action == describe;
       Action == execute ->
    {keep_state_and_data, postpone};

handle_event(internal, {fetch, _}, _, _) ->
    {keep_state_and_data, postpone};

handle_event(info, Msg, _, #{requests := Existing} = Data) ->
    case gen_statem:check_response(Msg, Existing, true) of
        {{reply, Reply}, Label, Updated} ->
            {keep_state,
             Data#{requests := Updated},
             nei({response, #{label => Label, reply => Reply}})};

        {{error, {Reason, ServerRef}}, Label, UpdatedRequests} ->
                {stop,
                 #{reason => Reason,
                   server_ref => ServerRef,
                   label => Label},
                 Data#{requests := UpdatedRequests}}
    end.


pub_fetch_sql(#{<<"schemaname">> := Schema,
                <<"tablename">> := Table} = Publication) ->
    ["select ",
     pub_fetch_columns(Publication),
     " from ",
     Schema,
     ".",
     Table,
     case maps:find(<<"rowfilter">>, Publication) of
        {ok, RowFilter} when RowFilter /= null ->
             [" where ", RowFilter];

        _Otherwise ->
             []
     end].


pub_fetch_columns(#{<<"attnames">> := Attributes}) ->
    lists:join(",", Attributes);

pub_fetch_columns(#{}) ->
    "*".
