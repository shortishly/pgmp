%% Copyright (c) 2022 Peter Morgan <peter.james.morgan@gmail.com>
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


-module(pgmp_rep_log_ets).


-behaviour(pgmp_rep_log).
-export([begin_transaction/1]).
-export([callback_mode/0]).
-export([commit/1]).
-export([delete/1]).
-export([handle_event/4]).
-export([init/1]).
-export([insert/1]).
-export([snapshot/1]).
-export([start_link/1]).
-export([truncate/1]).
-export([update/1]).
-import(pgmp_statem, [nei/1]).
-include_lib("kernel/include/logger.hrl").


start_link(Arg) ->
    gen_statem:start_link(?MODULE, [Arg], pgmp_config:options(?MODULE)).


snapshot(Arg) ->
    send_request(?FUNCTION_NAME, [id], Arg).


begin_transaction(Arg) ->
    send_request(?FUNCTION_NAME,
                 [commit_timestamp, final_lsn, xid, x_log],
                 Arg).


commit(Arg) ->
    send_request(?FUNCTION_NAME,
                 [commit_lsn, commit_timestamp, end_lsn, x_log],
                 Arg).


insert(Arg) ->
    send_request(?FUNCTION_NAME, [relation, tuple, x_log], Arg).


update(Arg) ->
    send_request(?FUNCTION_NAME, [relation, tuple, x_log], Arg).


delete(Arg) ->
    send_request(?FUNCTION_NAME, [relation, tuple, x_log], Arg).


truncate(Arg) ->
    send_request(?FUNCTION_NAME, [relations, x_log], Arg).


send_request(Action, Keys, Arg) ->
    send_request(
      maps:without(
        Keys,
        Arg#{request => {Action, maps:with(Keys, Arg)}})).


send_request(#{label := _} = Arg) ->
    pgmp_statem:send_request(Arg);

send_request(Arg) ->
    pgmp_statem:send_request(Arg#{label => ?MODULE}).


init([Arg]) ->
    process_flag(trap_exit, true),
    {ok, unready, Arg#{requests => gen_statem:reqids_new()}}.


callback_mode() ->
    handle_event_function.


handle_event({call, From},
             {begin_transaction, _},
             _,
             _) ->
    {keep_state_and_data, {reply, From, ok}};

handle_event({call, From},
             {commit, _},
             _,
             _) ->
    {keep_state_and_data, {reply, From, ok}};

handle_event({call, From},
             {insert,
              #{relation := Relation,
                x_log := _XLog,
                tuple := Tuple}},
             _,
             #{keys := Keys}) ->
    #{Relation := Positions} = Keys,
    insert_new(Relation, Tuple, Positions),
    {keep_state_and_data, {reply, From, ok}};

handle_event({call, From},
             {update,
              #{relation := Relation,
                x_log := _XLog,
                tuple := Tuple}},
              _,
              #{keys := Keys}) ->
    #{Relation := Positions} = Keys,
    update(Relation, Tuple, Positions),
    {keep_state_and_data, {reply, From, ok}};

handle_event({call, From},
             {delete,
              #{relation := Relation,
                x_log := _XLog,
                tuple := Tuple}},
              _,
              #{keys := Keys}) ->
    ets:delete(
      binary_to_atom(Relation),
      case Keys of
          #{Relation := [Primary]} ->
              element(Primary, Tuple);

          #{Relation := Composite} ->
              list_to_tuple([element(Pos, Tuple) || Pos <- Composite])
      end),
    {keep_state_and_data, {reply, From, ok}};

handle_event({call, From}, {truncate, #{relations := Relations}}, _, _) ->
    lists:foreach(
      fun
          (Relation) ->
              ets:delete_all_objects(binary_to_atom(Relation))
      end,
      Relations),
    {keep_state_and_data, {reply, From, ok}};


handle_event({call, Stream}, {snapshot, #{id := Id}}, _, Data) ->
    {keep_state,
     Data#{stream => Stream, keys => #{}},
     [nei(begin_transaction),
      nei({set_transaction_snapshot, Id}),
      nei(sync_publication_tables)]};

handle_event(internal,
             {response, #{reply := [{command_complete, 'begin'}]}},
             _,
             _) ->
    keep_state_and_data;

handle_event(internal,
             {response, #{reply := [{command_complete, commit}]}},
             _,
             #{stream := Stream} = Data) ->
    {next_state,
     ready,
     maps:without([stream], Data),
     {reply, Stream, ok}};

handle_event(internal,
             {response,
              #{label := sync_publication_tables = Label,
                reply := [{parse_complete, []}]}},
             _,
             #{config := #{publication := Publication}}) ->
    {keep_state_and_data,
     nei({bind, #{label => Label, args => [Publication]}})};

handle_event(internal,
             {response,
              #{label := sync_publication_tables = Label,
                reply := [{bind_complete, []}]}},
             _,
             _) ->
    {keep_state_and_data, nei({execute, #{label => Label}})};

handle_event(internal,
             {response, #{label := sync_publication_tables,
                          reply := [{command_complete, {select, 0}}]}},
             _,
             #{stream := Stream}) ->
    {stop_and_reply, normal, {reply, Stream, {error, no_tables}}};

handle_event(internal,
             {response, #{label := sync_publication_tables,
                          reply := [{row_description, Columns} | T]}},
             _,
             Data) ->
    {command_complete, {select, _}} = lists:last(T),
    {keep_state,
     Data#{tables => []},
     lists:map(
       fun
           ({data_row, Values}) ->
               nei({fetch, maps:from_list(lists:zip(Columns, Values))})
       end,
       lists:droplast(T))};

handle_event(internal,
             {fetch, #{<<"schemaname">> := Schema, <<"tablename">> := Table}},
             _,
             _) ->
    {keep_state_and_data,
     nei({parse,
          #{label => {fetch, #{schema => Schema, table => Table}},
            sql => <<"select i.indkey from pg_catalog.pg_index i"
                     ", pg_catalog.pg_namespace n"
                     ", pg_catalog.pg_class c"
                     " where "
                     "i.indrelid = c.oid"
                     " and "
                     "c.relnamespace = n.oid"
                     " and "
                     "n.nspname = $1"
                     " and "
                     "c.relname = $2">>}})};

handle_event(internal,
             {response,
              #{label := {fetch, #{schema := Schema, table := Table}} = Label,
                reply := [{parse_complete, []}]}},
             _,
             _) ->
    {keep_state_and_data,
     nei({bind, #{label => Label, args => [Schema, Table]}})};

handle_event(internal,
             {response,
              #{label := {fetch, #{schema := _, table := _}} = Label,
                reply := [{bind_complete, []}]}},
             _,
             _) ->
    {keep_state_and_data, nei({execute, #{label => Label}})};

handle_event(
  internal,
  {response,
   #{label := {fetch, #{schema := Schema, table := Table}},
     reply := [{row_description, [<<"indkey">>]},
               {data_row, [Key]},
               {command_complete, {select, 1}}]}},
  _,
  #{keys := Keys, tables := Tables} = Data) ->
    Name = binary_to_atom(Table),
    Label = {table,
             ets:new(
               Name,
               [{keypos,
                 case Key of
                     [Primary] ->
                         Primary;

                     _Composite ->
                         1
                 end},
                public,
                named_table])},
    {keep_state,
     Data#{keys := Keys#{Table => Key},
           tables := [Name | Tables]},
     [nei({parse,
           #{label => Label,
             sql => iolist_to_binary(
                      io_lib:format(
                        "select * from ~s.~s",
                        [Schema, Table]))}}),
      nei({bind, #{label => Label}}),
      nei({execute,
           #{label => Label,
             max_rows => pgmp_config:replication(
                           logical,
                           max_rows)}})]};


handle_event(internal,
             {response,
              #{label := {table, _},
                reply := [{parse_complete, []}]}},
             _,
             _) ->
    keep_state_and_data;

handle_event(internal,
             {response,
              #{label := {table, _},
                reply := [{bind_complete, []}]}},
             _,
             _) ->
    keep_state_and_data;

handle_event(internal,
             {response, #{label := {table, Table} = Label,
                          reply := [{row_description, _} | T]}},
             _,
             #{tables := Tables, keys := Keys} = Data) ->
    true = ets:insert_new(
             Table,
             lists:map(
               fun
                   ({data_row, Values}) ->
                       insert_or_update_tuple(
                         list_to_tuple(Values),
                         maps:get(atom_to_binary(Table), Keys))
               end,
               lists:droplast(T))),

    case lists:last(T) of
        {command_complete, {select, _}} = Result ->
            ?LOG_DEBUG(#{result => Result, length => length(T)}),
            case lists:delete(Table, Tables) of
                [] ->
                    {keep_state,
                     maps:without([tables], Data),
                     nei(commit)};

                Remaining ->
                    {keep_state, Data#{tables := Remaining}}
            end;

        {portal_suspended, _} = Result ->
            ?LOG_DEBUG(#{result => Result, length => length(T)}),
            {keep_state_and_data,
             nei({execute,
                  #{label => Label,
                    max_rows => pgmp_config:replication(
                                  logical,
                                  max_rows)}})}
    end;

handle_event(internal,
             {response, #{reply := [{command_complete, set}]}},
             _,
             _) ->
    keep_state_and_data;

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
            sql => iolist_to_binary(
                     io_lib:format(
                       "SET TRANSACTION SNAPSHOT '~s'",
                       [Id]))}})};

handle_event(internal,
             {Action, Arg},
             _,
             #{requests := Requests} = Data)
  when Action == query;
       Action == parse;
       Action == bind;
       Action == execute ->
    {keep_state,
     Data#{requests := pgmp_connection:Action(
                         Arg#{requests => Requests})}};

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


insert_new(Relation, Tuple, Keys) ->
    ets:insert_new(
      binary_to_atom(Relation),
      insert_or_update_tuple(Tuple, Keys)).

update(Relation, Tuple, Keys) ->
    ets:insert(
      binary_to_atom(Relation),
      insert_or_update_tuple(Tuple, Keys)).


insert_or_update_tuple(Tuple, [_]) ->
    Tuple;

insert_or_update_tuple(Tuple, Composite) ->
    list_to_tuple(
      [list_to_tuple([element(Pos, Tuple) || Pos <- Composite]) |
       lists:filtermap(
         fun
             ({Position, Value}) ->
                 case lists:member(Position, Composite) of
                     true ->
                         false;

                     false ->
                         {true, Value}
                 end
         end,
         lists:zip(
           lists:seq(1, tuple_size(Tuple)),
           tuple_to_list(Tuple)))]).
