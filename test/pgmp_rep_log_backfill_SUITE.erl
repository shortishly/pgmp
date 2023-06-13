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


-module(pgmp_rep_log_backfill_SUITE).


-compile(export_all).
-compile(nowarn_export_all).
-import(pgmp_rep_log_ets_common, [table_name/3]).
-include_lib("common_test/include/ct.hrl").


all() ->
    common:all(?MODULE).


init_per_suite(Config) ->
    _ = application:load(pgmp),

    application:set_env(pgmp, pgmp_replication_enabled, false),
    application:set_env(pgmp, pgmp_mm_trace, false),
    application:set_env(pgmp, pgmp_mm_log, true),
    application:set_env(pgmp, pgmp_mm_log_n, 50),

    application:set_env(pgmp, pgmp_rep_log_trace, false),

    {ok, _} = pgmp:start(),

    case version() of
        #{major := Major} when Major >= 15 ->
            Publication = alpha(5),
            Schema = alpha(5),
            Table = alpha(5),

            {ok, Sup} = pgmp_rep_sup:start_child(Publication),
            {_, Manager, worker, _} = pgmp_sup:get_child(Sup, manager),

            [{command_complete,
              create_schema}] = pgmp_connection_sync:query(
                                  #{sql => io_lib:format(
                                             "create schema ~s",
                                             [Schema])}),

            [{command_complete,
              create_publication}] = pgmp_connection_sync:query(
                                       #{sql => io_lib:format(
                                                  "create publication ~s for tables in schema ~s",
                                                  [Publication, Schema])}),

            [{command_complete,
              create_table}] = pgmp_connection_sync:query(
                                 #{sql => io_lib:format(
                                            "create table ~s.~s (k serial primary key, v text)",
                                            [Schema, Table])}),

            lists:map(
              fun
                  (_) ->
                      [{command_complete, 'begin'}] = pgmp_connection_sync:query(#{sql => "begin"}),

                      [{parse_complete, []}] = pgmp_connection_sync:parse(
                                                 #{sql => io_lib:format(
                                                            "insert into ~s.~s (v) values ($1) returning *",
                                                            [Schema, Table])}),

                      [{bind_complete, []}] = pgmp_connection_sync:bind(
                                                #{args => [alpha(5)]}),

                      [{row_description, _},
                       {data_row, [K | _] = Row},
                       {command_complete,
                        {insert, 1}}] =  pgmp_connection_sync:execute(#{}),

                      [{command_complete, commit}] = pgmp_connection_sync:query(#{sql => "commit"}),

                      ct:log("row: ~p~n", [Row]),

                      wait_for(
                        [list_to_tuple(Row)],
                        fun
                            () ->
                                try
                                    ets:lookup(table_name(Publication, Schema, Table), K)

                                catch
                                    error:badarg ->
                                        undefined
                                end
                        end),

                      list_to_tuple(Row)
              end,
              lists:seq(1, 50)),

            [{manager, Manager},
             {publication, Publication},
             {schema, Schema},
             {table, Table},
             {replica, table_name(Publication, Schema, Table)} | Config];

        Version ->
            {skip, {pg_version, Version}}
    end.


update_test(Config) ->
    Manager = ?config(manager, Config),
    Table = ?config(table, Config),
    Schema = ?config(schema, Config),
    Replica = ?config(replica, Config),

    {reply, ok} = gen_statem:receive_response(
                    pgmp_rep_log_ets:when_ready(
                      #{server_ref => Manager})),

    {K, _} = Existing = pick_one(ets:tab2list(Replica)),
    ct:log("existing: ~p~n", [Existing]),

    [{command_complete, 'begin'}] = pgmp_connection_sync:query(#{sql => "begin"}),

    [{parse_complete, []}] = pgmp_connection_sync:parse(
                               #{sql => io_lib:format(
                                          "update ~s.~s set v = $2 where k = $1 returning *",
                                          [Schema, Table])}),

    V = alpha(5),

    [{bind_complete, []}] = pgmp_connection_sync:bind(
                              #{args => [K, V]}),

    [{row_description, _},
     {data_row, [K, V] = Updated},
     {command_complete,
      {update, 1}}] =  pgmp_connection_sync:execute(#{}),

    ct:log("updated: ~p~n", [Updated]),

    [{command_complete, commit}] = pgmp_connection_sync:query(#{sql => "commit"}),

    wait_for(
      [list_to_tuple(Updated)],
      fun () ->
              ets:lookup(Replica, K)
      end).


delete_test(Config) ->
    Manager = ?config(manager, Config),
    Table = ?config(table, Config),
    Schema = ?config(schema, Config),
    Replica = ?config(replica, Config),

    {reply, ok} = gen_statem:receive_response(
                    pgmp_rep_log_ets:when_ready(
                      #{server_ref => Manager})),

    {K, V} = Existing = pick_one(ets:tab2list(Replica)),
    ct:log("existing: ~p~n", [Existing]),

    [{command_complete, 'begin'}] = pgmp_connection_sync:query(#{sql => "begin"}),

    [{parse_complete, []}] = pgmp_connection_sync:parse(
                               #{sql => io_lib:format(
                                          "delete from ~s.~s where k = $1 returning *",
                                          [Schema, Table])}),

    [{bind_complete, []}] = pgmp_connection_sync:bind(
                              #{args => [K]}),

    [{row_description, _},
     {data_row, [K, V] = Deleted},
     {command_complete,
      {delete, 1}}] =  pgmp_connection_sync:execute(#{}),

    ct:log("deleted: ~p~n", [Deleted]),

    [{command_complete, commit}] = pgmp_connection_sync:query(#{sql => "commit"}),

    wait_for(
      [],
      fun () ->
              ets:lookup(Replica, K)
      end).


insert_test(Config) ->
    Manager = ?config(manager, Config),
    Table = ?config(table, Config),
    Schema = ?config(schema, Config),
    Replica = ?config(replica, Config),

    {reply, ok} = gen_statem:receive_response(
                    pgmp_rep_log_ets:when_ready(
                      #{server_ref => Manager})),


    [{command_complete, 'begin'}] = pgmp_connection_sync:query(#{sql => "begin"}),

    [{parse_complete, []}] = pgmp_connection_sync:parse(
                               #{sql => io_lib:format(
                                          "insert into ~s.~s (v) values ($1) returning *",
                                          [Schema, Table])}),

    [{bind_complete, []}] = pgmp_connection_sync:bind(
                              #{args => [alpha(5)]}),

    [{row_description, _},
     {data_row, [K, _] = Inserted},
     {command_complete,
      {insert, 1}}] =  pgmp_connection_sync:execute(#{}),

    ct:log("inserted: ~p~n", [Inserted]),

    [{command_complete, commit}] = pgmp_connection_sync:query(#{sql => "commit"}),

    wait_for(
      [list_to_tuple(Inserted)],
      fun () ->
              ets:lookup(Replica, K)
      end).


wait_for(Expected, Check) ->
    ct:log("expected: ~p, check: ~p~n", [Expected, Check]),
    ?FUNCTION_NAME(Expected, Check, 5).

wait_for(Expected, Check, 0 = N) ->
    ct:log("expected: ~p,~ncheck: ~p,~nn: ~p~n",
           [Expected, Check, N]),
    case Check() of
        Expected ->
            Expected;

        Unexpected ->
            ct:log("expected: ~p~ncheck: ~p~nn: ~p~nactual: ~p~n",
                   [Expected, Check, N, Unexpected]),
            Expected = Unexpected
    end;

wait_for(Expected, Check, N) ->
    ct:log("expected: ~p,~ncheck: ~p,~nn: ~p~n",
           [Expected, Check, N]),
    case Check() of
        Expected ->
            Expected;

        Unexpected ->
            ct:log("expected: ~p,~ncheck: ~p,~nn: ~p,~nactual: ~p~n",
                   [Expected, Check, N, Unexpected]),
            timer:sleep(timer:seconds(1)),
            ?FUNCTION_NAME(Expected, Check, N - 1)
    end.


end_per_suite(Config) ->
    Schema = ?config(schema, Config),
    ct:log("~s: ~p~n",
           [Schema,
            pgmp_connection_sync:query(
              #{sql => iolist_to_binary(
                         io_lib:format(
                           "drop schema ~s cascade",
                           [Schema]))})]),

    ok = application:stop(pgmp).


alpha(N) ->
    list_to_binary(pick(N, lists:seq($a, $z))).


pick_one(Pool) ->
    [Victim] = pick(1, Pool),
    Victim.


pick(N, Pool) ->
    ?FUNCTION_NAME(N, Pool, []).


pick(0, _, A) ->
    A;

pick(N, Pool, A) ->
    ?FUNCTION_NAME(N - 1,
                   Pool,
                   [lists:nth(rand:uniform(length(Pool)), Pool) | A]).


version() ->
    #{<<"server_version">> := Version} = pgmp_connection_sync:parameters(#{}),
    pgmp_util:semantic_version(Version).
