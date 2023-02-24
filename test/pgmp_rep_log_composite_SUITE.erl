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


-module(pgmp_rep_log_composite_SUITE).


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

    application:set_env(pgmp, pgmp_rep_log_trace, true),

    {ok, _} = pgmp:start(),

    Table = alpha(5),

    [{command_complete,
      create_table}] = pgmp_connection_sync:query(
                         #{sql => io_lib:format(
                                    "create table ~s (x serial, v text, y serial, primary key (x, y))",
                                    [Table])}),


    Publication = alpha(5),

    [{command_complete,
      create_publication}] = pgmp_connection_sync:query(
                               #{sql => io_lib:format(
                                          "create publication ~s for table ~s",
                                          [Publication, Table])}),

    [{command_complete, 'begin'}] = pgmp_connection_sync:query(#{sql => "begin"}),


    [{parse_complete, []}] = pgmp_connection_sync:parse(
                               #{sql => io_lib:format(
                                          "insert into ~s (v) values ($1) returning *",
                                          [Table])}),

    lists:map(
      fun
          (_) ->
              [{bind_complete, []}] = pgmp_connection_sync:bind(
                                        #{args => [alpha(5)]}),

              [{row_description, _},
               {data_row, Row},
               {command_complete,
                {insert, 1}}] =  pgmp_connection_sync:execute(#{}),

              list_to_tuple(Row)
      end,
      lists:seq(1, 50)),

    [{parse_complete,[]}] =  pgmp_connection_sync:parse(
                               #{sql => "select pubname,schemaname,tablename "
                                 "from pg_catalog.pg_publication_tables "
                                 "where pubname = $1"}),

    [{bind_complete, []}] = pgmp_connection_sync:bind(#{args => [Publication]}),

    [{row_description,
      [<<"pubname">>,
       <<"schemaname">>,
       <<"tablename">>]},
     {data_row,
      [Publication,
       Schema,
       Table]},
     {command_complete,
      {select,1}}] = pgmp_connection_sync:execute(#{}),

    [{command_complete, commit}] = pgmp_connection_sync:query(#{sql => "commit"}),

    {ok, Sup} = pgmp_rep_sup:start_child(Publication),

    {_, Manager, worker, _} = pgmp_sup:get_child(Sup, manager),

    [{manager, Manager},
     {publication, Publication},
     {schema, Schema},
     {table, Table},
     {replica, table_name(Publication, Schema, Table)} | Config].


update_test(Config) ->
    Manager = ?config(manager, Config),
    Table = ?config(table, Config),
    Replica = ?config(replica, Config),

    {reply, ok} = gen_statem:receive_response(
                    pgmp_rep_log_ets:when_ready(
                      #{server_ref => Manager})),

    {{X, Y}, _} = Existing = pick_one(ets:tab2list(Replica)),
    ct:log("existing: ~p~n", [Existing]),

    [{command_complete, 'begin'}] = pgmp_connection_sync:query(#{sql => "begin"}),

    [{parse_complete, []}] = pgmp_connection_sync:parse(
                               #{sql => io_lib:format(
                                          "update ~s set v = $3 where x = $1 and y = $2 returning *",
                                          [Table])}),

    V = alpha(5),

    [{bind_complete, []}] = pgmp_connection_sync:bind(
                              #{args => [X, Y, V]}),

    [{row_description, _},
     {data_row, [X, V, Y] = Updated},
     {command_complete,
      {update, 1}}] =  pgmp_connection_sync:execute(#{}),

    ct:log("updated: ~p~n", [Updated]),

    [{command_complete, commit}] = pgmp_connection_sync:query(#{sql => "commit"}),

    wait_for(
      [{{X, Y}, V}],
      fun () ->
              ets:lookup(Replica, {X, Y})
      end).


delete_test(Config) ->
    Manager = ?config(manager, Config),
    Table = ?config(table, Config),
    Replica = ?config(replica, Config),

    {reply, ok} = gen_statem:receive_response(
                    pgmp_rep_log_ets:when_ready(
                      #{server_ref => Manager})),

    {{X, Y}, V} = Existing = pick_one(ets:tab2list(Replica)),
    ct:log("existing: ~p~n", [Existing]),

    [{command_complete, 'begin'}] = pgmp_connection_sync:query(#{sql => "begin"}),

    [{parse_complete, []}] = pgmp_connection_sync:parse(
                               #{sql => io_lib:format(
                                          "delete from ~s where x = $1 and y = $2 returning *",
                                          [Table])}),

    [{bind_complete, []}] = pgmp_connection_sync:bind(
                              #{args => [X, Y]}),

    [{row_description, _},
     {data_row, [X, V, Y] = Deleted},
     {command_complete,
      {delete, 1}}] =  pgmp_connection_sync:execute(#{}),

    ct:log("deleted: ~p~n", [Deleted]),

    [{command_complete, commit}] = pgmp_connection_sync:query(#{sql => "commit"}),

    wait_for(
      [],
      fun () ->
              ets:lookup(Replica, {X, Y})
      end).


insert_test(Config) ->
    Manager = ?config(manager, Config),
    Table = ?config(table, Config),
    Replica = ?config(replica, Config),

    {reply, ok} = gen_statem:receive_response(
                    pgmp_rep_log_ets:when_ready(
                      #{server_ref => Manager})),


    [{command_complete, 'begin'}] = pgmp_connection_sync:query(#{sql => "begin"}),

    [{parse_complete, []}] = pgmp_connection_sync:parse(
                               #{sql => io_lib:format(
                                          "insert into ~s (v) values ($1) returning *",
                                          [Table])}),

    [{bind_complete, []}] = pgmp_connection_sync:bind(
                              #{args => [alpha(5)]}),

    [{row_description, _},
     {data_row, [X, V, Y] = Inserted},
     {command_complete,
      {insert, 1}}] =  pgmp_connection_sync:execute(#{}),

    ct:log("inserted: ~p~n", [Inserted]),

    [{command_complete, commit}] = pgmp_connection_sync:query(#{sql => "commit"}),

    wait_for(
      [{{X, Y}, V}],
      fun () ->
              ets:lookup(Replica, {X, Y})
      end).


truncate_test(Config) ->
    Manager = ?config(manager, Config),
    Table = ?config(table, Config),
    Replica = ?config(replica, Config),

    {reply, ok} = gen_statem:receive_response(
                    pgmp_rep_log_ets:when_ready(
                      #{server_ref => Manager})),

    [{command_complete, 'begin'}] = pgmp_connection_sync:query(#{sql => "begin"}),

    [{parse_complete, []}] = pgmp_connection_sync:parse(
                               #{sql => io_lib:format("truncate table ~s", [Table])}),

    [{bind_complete, []}] = pgmp_connection_sync:bind(#{args => []}),

    [{command_complete, truncate_table}] =  pgmp_connection_sync:execute(#{}),

    [{command_complete, commit}] = pgmp_connection_sync:query(#{sql => "commit"}),

    wait_for(
      0,
      fun () ->
              ets:info(Replica, size)
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
    Table = ?config(table, Config),
    _Publication = ?config(publication, Config),

    ct:log("~s: ~p~n",
           [Table,
            pgmp_connection_sync:query(
              #{sql => iolist_to_binary(
                         io_lib:format(
                           "drop table ~s cascade",
                           [Table]))})]),

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
