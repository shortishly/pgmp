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


-module(pgmp_rep_log_SUITE).


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
    {ok, _} = pgmp:start(),

    Table = alpha(5),

    ct:log("~s: ~p~n",
           [Table,
            pgmp_connection_sync:query(
              #{sql => iolist_to_binary(
                         io_lib:format(
                           "create table ~s (x int, y text)",
                           [Table]))})]),

    Publication = alpha(5),

    ct:log("~s: ~p~n",
           [Publication,
            pgmp_connection_sync:query(
              #{sql => iolist_to_binary(
                         io_lib:format(
                           "create publication ~s for table ~s",
                           [Publication, Table]))})]),

    [{publication, Publication}, {table, Table} | Config].


end_per_suite(Config) ->
    Table = ?config(table, Config),

    ct:log("~s: ~p~n",
           [Table,
            pgmp_connection_sync:query(
              #{sql => iolist_to_binary(
                         io_lib:format(
                           "drop table ~s",
                           [Table]))})]),

    ok = application:stop(pgmp).


alpha(N) ->
    pick(N, lists:append(lists:seq($a, $z), lists:seq($A, $Z))).


abc_test(_Config) ->
    a == a.


pick(N, Pool) ->
    ?FUNCTION_NAME(N, Pool, []).


pick(0, _, A) ->
    list_to_binary(A);

pick(N, Pool, A) ->
    ?FUNCTION_NAME(N - 1,
                   Pool,
                   [lists:nth(rand:uniform(length(Pool)), Pool) | A]).

