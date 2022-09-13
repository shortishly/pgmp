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


-module(pgmp_prop_types_SUITE).


-compile(export_all).
-compile(nowarn_export_all).
-include_lib("common_test/include/ct.hrl").


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
    ct_property_test:init_per_suite(Config).


end_per_suite(_Config) ->
    ok = application:stop(pgmp).


numeric_test(Config) ->
    t(?FUNCTION_NAME, Config).


smallint_test(Config) ->
    t(?FUNCTION_NAME, Config).


integer_test(Config) ->
    t(?FUNCTION_NAME, Config).


oid_test(Config) ->
    t(?FUNCTION_NAME, Config).


regproc_test(Config) ->
    t(?FUNCTION_NAME, Config).


regprocedure_test(Config) ->
    t(?FUNCTION_NAME, Config).


regoper_test(Config) ->
    t(?FUNCTION_NAME, Config).


regoperator_test(Config) ->
    t(?FUNCTION_NAME, Config).


regclass_test(Config) ->
    t(?FUNCTION_NAME, Config).


regtype_test(Config) ->
    t(?FUNCTION_NAME, Config).


regconfig_test(Config) ->
    t(?FUNCTION_NAME, Config).


regdictionary_test(Config) ->
    t(?FUNCTION_NAME, Config).


time_test(Config) ->
    t(?FUNCTION_NAME, Config).


date_test(Config) ->
    t(?FUNCTION_NAME, Config).


macaddr8_test(Config) ->
    t(?FUNCTION_NAME, Config).


macaddr_test(Config) ->
    t(?FUNCTION_NAME, Config).


inet_test(Config) ->
    t(?FUNCTION_NAME, Config).


point_test(Config) ->
    t(?FUNCTION_NAME, Config).


polygon_test(Config) ->
    t(?FUNCTION_NAME, Config).


path_test(Config) ->
    t(?FUNCTION_NAME, Config).


circle_test(Config) ->
    t(?FUNCTION_NAME, Config).


lseg_test(Config) ->
    t(?FUNCTION_NAME, Config).


box_test(Config) ->
    t(?FUNCTION_NAME, Config).


line_test(Config) ->
    t(?FUNCTION_NAME, Config).


timestamp_test(Config) ->
    t(?FUNCTION_NAME, Config).


timestamptz_test(Config) ->
    t(?FUNCTION_NAME, Config).


%% oidvector_test(Config) ->
%%    t(?FUNCTION_NAME, Config).


integer_array_test(Config) ->
    t(?FUNCTION_NAME, Config).


bigint_test(Config) ->
    t(?FUNCTION_NAME, Config).


money_test(Config) ->
    t(?FUNCTION_NAME, Config).


real_test(Config) ->
    t(?FUNCTION_NAME, Config).


double_precision_test(Config) ->
    t(?FUNCTION_NAME, Config).


bytea_test(Config) ->
    t(?FUNCTION_NAME, Config).


boolean_test(Config) ->
    t(?FUNCTION_NAME, Config).


bit_varying_test(Config) ->
    t(?FUNCTION_NAME, Config).


varchar_test(Config) ->
    t(?FUNCTION_NAME, Config).


text_test(Config) ->
    t(?FUNCTION_NAME, Config).


uuid_test(Config) ->
    t(?FUNCTION_NAME, Config).


t(FunctionName, Config) ->
    Names = lists:droplast(
              pgmp_util:split_on_snake_case(FunctionName)),
    Property = pgmp_util:snake_case([prop | Names]),
    ct_property_test:quickcheck(
      pgmp_prop_types:Property(),
      Config).
