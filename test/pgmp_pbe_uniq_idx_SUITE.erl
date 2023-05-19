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


-module(pgmp_pbe_uniq_idx_SUITE).


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

    application:set_env(pgmp, pgmp_rep_log_trace, true),

    {ok, _} = pgmp:start(),

    logger:set_handler_config(
      default,
      #{formatter => {logger_formatter,
                      #{template => [[logger_formatter, header],
                                     {pid, [" ", pid, ""], ""},
                                     {mfa, [" ", mfa, ":", line], ""},
                                     "\n",
                                     msg,
                                     "\n"],
                        legacy_header => true,
                        single_line => false}}}),

    logger:set_module_level([], debug),

    Table = alpha(5),

    [{command_complete,
      create_table}] = pgmp_connection_sync:query(
                         #{sql => io_lib:format(
                                    "create table ~s ("
                                    "k text primary key,"
                                    "v text"
                                    ")",
                                    [Table])}),

    [{command_complete,
      create_index}] = pgmp_connection_sync:query(
                         #{sql => io_lib:format(
                                    "create unique index ~s_idx on ~s (v)",
                                    [Table, Table])}),
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
    V = alpha(5),

    [{command_complete, 'begin'}] = pgmp_connection_sync:query(
                                      #{sql => "begin isolation level repeatable read"}),

    [{parse_complete, []}] = pgmp_connection_sync:parse(
                               #{sql => io_lib:format(
                                          "insert into ~s (k, v) values ($1, $2) returning *",
                                          [Table])}),

    [{bind_complete, []}] = pgmp_connection_sync:bind(#{args => [K, V]}),

    [{row_description, _},
     {data_row, [K, V]},
     {command_complete,
      {insert, 1}}] =  pgmp_connection_sync:execute(#{}),

    [{command_complete, commit}] = pgmp_connection_sync:query(
                                     #{sql => "commit"}).


pbe_sync_test(Config) ->
    Table = ?config(table, Config),

    K = alpha(5),
    V = alpha(5),

    [{parse_complete, []}] = pgmp_connection_sync:parse(
                               #{sql => io_lib:format(
                                          "insert into ~s (k, v) values ($1, $2) returning *",
                                          [Table])}),

    [{bind_complete, []}] = pgmp_connection_sync:bind(#{args => [K, V]}),

    [{row_description, _},
     {data_row, [K, V]},
     {command_complete,
      {insert, 1}}] =  pgmp_connection_sync:execute(#{}),

    ok = pgmp_connection_sync:sync(#{}).


duplicate_key_test(Config) ->
    Table = ?config(table, Config),

    K0 = alpha(5),
    V0 = alpha(5),
    ct:log("~s: ~p~n", [Table, {K0, V0}]),

    [{command_complete, 'begin'}] = pgmp_connection_sync:query(
                                      #{sql => "begin isolation level repeatable read"}),

    [{parse_complete, []}] = pgmp_connection_sync:parse(
                               #{sql => io_lib:format(
                                          "insert into ~s (k, v) values ($1, $2) returning *",
                                          [Table])}),

    [{bind_complete, []}] = pgmp_connection_sync:bind(#{args => [K0, V0]}),

    [{row_description, _},
     {data_row, [K0, V0]},
     {command_complete,
      {insert, 1}}] =  pgmp_connection_sync:execute(#{}),

    K1 = alpha(5),
    ct:log("~s: ~p~n", [Table, {K1, V0}]),

    [{bind_complete, []}] = pgmp_connection_sync:bind(#{args => [K1, V0]}),

    ConstraintName = iolist_to_binary([Table, "_idx"]),

    [{error_response,
      #{constraint_name := ConstraintName,
        message := <<"duplicate key value", _/bytes>>}}] = pgmp_connection_sync:execute(#{}),

    [{command_complete, rollback}] = pgmp_connection_sync:query(
                                     #{sql => "commit"}).


use_prepared_statement_after_error_test(Config) ->
    Table = ?config(table, Config),

    K0 = alpha(5),
    V0 = alpha(5),
    ct:log("~s: ~p~n", [Table, {K0, V0}]),

    [{command_complete, 'begin'}] = pgmp_connection_sync:query(
                                      #{sql => "begin isolation level repeatable read"}),

    [{parse_complete, []}] = pgmp_connection_sync:parse(
                               #{sql => io_lib:format(
                                          "insert into ~s (k, v) values ($1, $2) returning *",
                                          [Table])}),

    [{bind_complete, []}] = pgmp_connection_sync:bind(#{args => [K0, V0]}),

    [{row_description, _},
     {data_row, [K0, V0]},
     {command_complete,
      {insert, 1}}] =  pgmp_connection_sync:execute(#{}),

    K1 = alpha(5),
    ct:log("~s: ~p~n", [Table, {K1, V0}]),

    [{bind_complete, []}] = pgmp_connection_sync:bind(#{args => [K1, V0]}),

    ConstraintName = iolist_to_binary([Table, "_idx"]),

    [{error_response,
      #{constraint_name := ConstraintName,
        message := <<"duplicate key value", _/bytes>>}}] =
        pgmp_connection_sync:execute(#{}),

    [{error_response,
      #{code := <<"25P02">>,
        message := <<"current transaction is aborted", _/bytes>>}}] =
        pgmp_connection_sync:parse(
          #{sql => io_lib:format(
                     "insert into ~s (k, v) values ($1, $2) returning *",
                     [Table])}),

    [{command_complete, rollback}] = pgmp_connection_sync:query(
                                       #{sql => "commit"}).


bind_prepared_statement_after_error_test(Config) ->
    Table = ?config(table, Config),

    K0 = alpha(5),
    V0 = alpha(5),
    ct:log("~s: ~p~n", [Table, {K0, V0}]),

    [{command_complete, 'begin'}] = pgmp_connection_sync:query(
                                      #{sql => "begin isolation level repeatable read"}),

    [{parse_complete, []}] = pgmp_connection_sync:parse(
                               #{sql => io_lib:format(
                                          "insert into ~s (k, v) values ($1, $2) returning *",
                                          [Table])}),

    [{bind_complete, []}] = pgmp_connection_sync:bind(#{args => [K0, V0]}),

    [{row_description, _},
     {data_row, [K0, V0]},
     {command_complete,
      {insert, 1}}] =  pgmp_connection_sync:execute(#{}),

    K1 = alpha(5),
    ct:log("~s: ~p~n", [Table, {K1, V0}]),

    [{bind_complete, []}] = pgmp_connection_sync:bind(#{args => [K1, V0]}),

    ConstraintName = iolist_to_binary([Table, "_idx"]),

    [{error_response,
      #{constraint_name := ConstraintName,
        message := <<"duplicate key value", _/bytes>>}}] =
        pgmp_connection_sync:execute(#{}),

    K2 = alpha(5),
    V2 = alpha(5),
    ct:log("~s: ~p~n", [Table, {K2, V2}]),

    [{error_response,
      #{code := <<"25P02">>,
        message :=
            <<"current transaction is aborted, "
              "commands ignored until end of transaction block">>,
        severity := error}}] =
        pgmp_connection_sync:bind(#{args => [K2, V2]}),

    [{command_complete, rollback}] = pgmp_connection_sync:query(
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
