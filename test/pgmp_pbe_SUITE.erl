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


-module(pgmp_pbe_SUITE).


-compile(export_all).
-compile(nowarn_export_all).
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

    Table = alpha(5),

    [{command_complete,
      create_table}] = pgmp_connection_sync:query(
                         #{sql => io_lib:format(
                                    "create table ~s ("
                                    "k text primary key"
                                    ")",
                                    [Table])}),
    [{table, Table} | Config].


end_per_suite(Config) ->
    Table = ?config(table, Config),

    ct:log("~s: ~p~n",
           [Table,
            pgmp_connection_sync:query(
              #{sql => iolist_to_binary(
                         io_lib:format(
                           "drop table ~s cascade",
                           [Table]))})]),

    ok = application:stop(pgmp).


pbe_test(Config) ->
    Table = ?config(table, Config),

    K = alpha(5),

    [{command_complete, 'begin'}] = pgmp_connection_sync:query(
                                      #{sql => "begin isolation level repeatable read"}),

    [{parse_complete, []}] = pgmp_connection_sync:parse(
                               #{sql => io_lib:format(
                                          "insert into ~s (k) values ($1) returning *",
                                          [Table])}),

    [{bind_complete, []}] = pgmp_connection_sync:bind(#{args => [K]}),

    [{row_description, _},
     {data_row, [K]},
     {command_complete,
      {insert, 1}}] =  pgmp_connection_sync:execute(#{}),

    [{command_complete, commit}] = pgmp_connection_sync:query(
                                     #{sql => "commit"}).


pbe_sync_test(Config) ->
    Table = ?config(table, Config),

    K = alpha(5),

    [{parse_complete, []}] = pgmp_connection_sync:parse(
                               #{sql => io_lib:format(
                                          "insert into ~s (k) values ($1) returning *",
                                          [Table])}),

    [{bind_complete, []}] = pgmp_connection_sync:bind(#{args => [K]}),

    [{row_description, _},
     {data_row, [K]},
     {command_complete,
      {insert, 1}}] =  pgmp_connection_sync:execute(#{}),

    ok = pgmp_connection_sync:sync(#{}).


bind_without_parse_test(_Config) ->
    K = alpha(5),

    Response = pgmp_connection_sync:bind(#{args => [K]}),

    ct:pal("~p~n", [Response]),

    [{error_response,
      #{code := <<"26000">>,
        severity := error,
        message := <<"unnamed prepared statement does not exist">>}}]
        = Response.


execute_without_parse_bind_test(_Config) ->
    [{error_response,
      #{code := <<"34000">>,
        message := <<"portal \"\" does not exist">>,
        severity := error}}] =
        pgmp_connection_sync:execute(#{}).


duplicate_key_test(Config) ->
    Table = ?config(table, Config),

    K0 = alpha(5),

    [{command_complete, 'begin'}] = pgmp_connection_sync:query(
                                      #{sql => "begin isolation level repeatable read"}),

    [{parse_complete, []}] = pgmp_connection_sync:parse(
                               #{sql => io_lib:format(
                                          "insert into ~s (k) values ($1) returning *",
                                          [Table])}),

    [{bind_complete, []}] = pgmp_connection_sync:bind(#{args => [K0]}),

    [{row_description, _},
     {data_row, [K0]},
     {command_complete,
      {insert, 1}}] =  pgmp_connection_sync:execute(#{}),

    [{command_complete, {insert, 0}}] =  pgmp_connection_sync:execute(#{}),

    [{command_complete, commit}] = pgmp_connection_sync:query(
                                     #{sql => "commit"}).


use_prepared_statement_after_error_test(Config) ->
    Table = ?config(table, Config),

    K0 = alpha(5),
    ct:log("~s: ~p~n", [Table, K0]),

    [{command_complete, 'begin'}] = pgmp_connection_sync:query(
                                      #{sql => "begin isolation level repeatable read"}),

    [{parse_complete, []}] = pgmp_connection_sync:parse(
                               #{sql => io_lib:format(
                                          "insert into ~s (k) values ($1) returning *",
                                          [Table])}),

    [{bind_complete, []}] = pgmp_connection_sync:bind(#{args => [K0]}),

    [{row_description, _},
     {data_row, [K0]},
     {command_complete,
      {insert, 1}}] =  pgmp_connection_sync:execute(#{}),

    [{command_complete, {insert, 0}}] =  pgmp_connection_sync:execute(#{}),

    K1 = alpha(5),
    ct:log("~s: ~p~n", [Table, K1]),

    [{bind_complete, []}] = pgmp_connection_sync:bind(#{args => [K1]}),

    [{row_description, _},
     {data_row, [K1]},
     {command_complete,
      {insert, 1}}] =  pgmp_connection_sync:execute(#{}),

    [{command_complete, commit}] = pgmp_connection_sync:query(
                                     #{sql => "commit"}).


alpha(N) ->
    list_to_binary(pick(N, lists:seq($a, $z))).

pick(N, Pool) ->
    ?FUNCTION_NAME(N, Pool, []).


pick(0, _, A) ->
    A;

pick(N, Pool, A) ->
    ?FUNCTION_NAME(N - 1,
                   Pool,
                   [lists:nth(rand:uniform(length(Pool)), Pool) | A]).
