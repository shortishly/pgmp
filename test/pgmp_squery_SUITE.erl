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


-module(pgmp_squery_SUITE).


-compile(export_all).
-compile(nowarn_export_all).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").


all() ->
    common:all(?MODULE).


init_per_suite(Config) ->
    _ = application:load(pgmp),
    application:set_env(pgmp, pgmp_replication_enabled, false),
    application:set_env(pgmp, pgmp_mm_trace, false),
    application:set_env(pgmp, pgmp_connection_trace, false),
    application:set_env(pgmp, pgmp_mm_log, true),
    application:set_env(pgmp, pgmp_mm_log_n, 50),
    {ok, _} = pgmp:start(),
    Config.


end_per_suite(_Config) ->
    ok = application:stop(pgmp).


query_test(_Config) ->
    ?assertMatch(
       [{row_description, _},
        {data_row, [4]},
        {command_complete, {select, 1}}],
       pgmp_connection_sync:query(
         #{sql => "select 2 + 2"})).


begin_test(_Config) ->
    [{command_complete,'begin'}] = pgmp_connection_sync:query(
                                     #{sql => "begin work"}).


begin_select_commit_test(_Config) ->
    [{command_complete,'begin'}] = pgmp_connection_sync:query(
                                     #{sql => "begin work"}),

    [{row_description, _},
     {data_row, [4]},
     {command_complete, {select, 1}}] = pgmp_connection_sync:query(
                                          #{sql => "select 2 + 2"}),

    [{command_complete, commit}] = pgmp_connection_sync:query(
                                     #{sql => "commit"}).


query_syntax_test(_Config) ->
    [{error_response,
      #{code := <<"42703">>,
        file_name := <<"parse_relation.c">>,
        line := _,
        message := <<"column \"a\" does not exist">>,
        position := 12,
        routine := <<"errorMissingColumn">>,
        severity := error,
        severity_localized := <<"ERROR">>}}] = pgmp_connection_sync:query(
                                                 #{sql => "select 2 + a"}).
