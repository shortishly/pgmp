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


-module(pgmp_equery_SUITE).


-compile(export_all).
-compile(nowarn_export_all).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").


all() ->
    common:all(?MODULE).


init_per_suite(Config) ->
    _ = application:load(pgmp),
    application:set_env(pgmp, pgmp_replication_enabled, false),
    application:set_env(pgmp, pgmp_mm_trace, true),
    application:set_env(pgmp, pgmp_connection_trace, false),
    application:set_env(pgmp, pgmp_mm_log, true),
    application:set_env(pgmp, pgmp_mm_log_n, 50),
    {ok, _} = pgmp:start(),
    Config.


end_per_suite(_Config) ->
    ok = application:stop(pgmp).


parse_test(_Config) ->
    [{parse_complete, []}] = pgmp_connection_sync:parse(
                               #{sql => "select 2 + 2"}).


parse_bind_test(_Config) ->
    [{parse_complete, []}] = pgmp_connection_sync:parse(
                               #{sql => "select 2 + 2"}),
    [{bind_complete, []}] = pgmp_connection_sync:bind(
                              #{args => []}).

bind_badarg_type_test(_Config) ->
    [{parse_complete, []}] = pgmp_connection_sync:parse(
                               #{sql => "select 2 + $1"}),
    [{error, badarg}] = pgmp_connection_sync:bind(
                              #{args => [<<"abc">>]}).

bind_error_response_test(_Config) ->
    [{parse_complete, []}] = pgmp_connection_sync:parse(
                               #{sql => "select 2 / $1"}),
    [{error_response, _}] = pgmp_connection_sync:bind(
                              #{args => [0]}).

parse_bind_execute_test(_Config) ->
    ?assertMatch(
       [{parse_complete, []}],
       pgmp_connection_sync:parse(
         #{sql => "select 2 + 2"})),
    ?assertMatch(
       [{bind_complete, []}],
       pgmp_connection_sync:bind(
         #{args => []})),
    ?assertMatch(
       [{row_description, [<<"?column?">>]},
        {data_row, [4]},
        {command_complete, {select, 1}}],
       pgmp_connection_sync:execute(#{})).


parse_syntax_test(_Config) ->
    ?assertMatch(
       [{error_response,
         #{code := <<"42703">>,
           file_name := <<"parse_relation.c">>,
           line := _,
           message := <<"column \"a\" does not exist">>,
           position := 12,
           routine := <<"errorMissingColumn">>,
           severity := error,
           severity_localized := <<"ERROR">>}}],
       pgmp_connection_sync:parse(
         #{sql => "select 2 + a"})).
