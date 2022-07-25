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


-module(pgmp_replication_logical_snapshot_manager).


-export([callback_mode/0]).
-export([handle_event/4]).
-export([init/1]).
-export([snapshot/1]).
-export([start_link/1]).
-import(pgmp_statem, [nei/1]).
-include_lib("kernel/include/logger.hrl").


start_link(Arg) ->
    gen_statem:start_link(?MODULE, [Arg], pgmp_config:options(?MODULE)).


snapshot(#{id := Id} = Arg) ->
    send_request(
      maps:without(
        [id],
        Arg#{request => {?FUNCTION_NAME, Id}})).


send_request(#{label := _} = Arg) ->
    pgmp_statem:send_request(Arg);

send_request(Arg) ->
    pgmp_statem:send_request(Arg#{label => ?MODULE}).


init([Arg]) ->
    process_flag(trap_exit, true),
    {ok, unready, Arg#{requests => gen_statem:reqids_new()}, nei(peer)}.


callback_mode() ->
    handle_event_function.


handle_event(internal,
             peer,
             _,
             #{ancestors := [Supervisor | _]} = Data) ->
    case pgmp_sup:get_child(Supervisor, mm) of
        {_, PID, worker, _} when is_pid(PID) ->
            {next_state,
             ready,
             Data#{mm => PID}};

        {_, _, _, _} = Reason ->
            {stop, Reason};

        false ->
            {stop, peer_not_found}
    end;


handle_event({call, {Stream, _} = From}, {snapshot, Id}, _, Data) ->
    {keep_state,
     Data#{stream => Stream},
     [nei(begin_transaction),
      nei({set_transaction_snapshot, Id}),
      nei(sync_publication_tables),
      {reply, From, ok}]};

handle_event(internal,
             {response, #{reply := [{command_complete, 'begin'}]}},
             _,
             _) ->
    keep_state_and_data;

handle_event(internal,
             {response, #{reply := [{command_complete, commit}]}},
             _,
             _) ->
    {keep_state_and_data, nei(start_replication)};

handle_event(internal,
             {response, #{label := start_replication, reply := ok}},
             _,
             _) ->
    keep_state_and_data;

handle_event(internal,
             {response, #{label := sync_publication_tables,
                          reply := [{row_description, Columns} | T]}},
             _,
             _) ->
    {command_complete, {select, _}} = lists:last(T),
    {keep_state_and_data,
     lists:map(
       fun
           ({data_row, Values}) ->
               nei({fetch, maps:from_list(lists:zip(Columns, Values))})
       end,
       lists:droplast(T)) ++ [nei(commit)]};

handle_event(internal,
             start_replication = Label,
             _,
             #{requests := Requests, stream := Stream} = Data) ->
    {keep_state,
     Data#{requests := pgmp_mm:start_replication(
                         #{server_ref => Stream,
                           label => Label,
                           requests => Requests})}};

handle_event(internal,
             {fetch, #{<<"schemaname">> := Schema, <<"tablename">> := Table}},
             _,
             _) ->
    {keep_state_and_data,
     nei({query,
          #{label => {table, ets:new(binary_to_atom(Table), [public, named_table])},
            sql => iolist_to_binary(
                     io_lib:format(
                       "select * from ~s.~s", [Schema, Table]))}})};


handle_event(internal,
             {response, #{label := {table, Table},
                          reply := [{row_description, _} | T]}},
             _,
             _) ->
    {command_complete, {select, _}} = lists:last(T),
    ets:insert_new(
      Table,
      lists:map(
        fun
            ({data_row, Values}) ->
                list_to_tuple(Values)
        end,
        lists:droplast(T))),
    keep_state_and_data;

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
     nei({query,
          #{label => Label,
            sql => iolist_to_binary(
                     io_lib:format(
                       "select * from pg_catalog.pg_publication_tables "
                       "where pubname = '~s'",
                       [pgmp_config:replication(logical, publication_names)]))}})};

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
             {query, Arg},
             _,
             #{mm := MM, requests := Requests} = Data) ->
    {keep_state,
     Data#{requests := pgmp_mm:query(
                         Arg#{server_ref => MM, requests => Requests})}};

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
