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


-module(type_prop_SUITE).


-compile(export_all).
-compile(nowarn_export_all).
-include_lib("common_test/include/ct.hrl").


all() ->
    common:all(?MODULE).


init_per_suite(Config) ->
    _ = application:load(pgmp),
    application:set_env(pgmp, pgmp_replication_enabled, false),
    application:set_env(pgmp, pgmp_mm_trace, false),
    {ok, _} = pgmp:start(),
    ct_property_test:init_per_suite(Config).


end_per_suite(_Config) ->
    ok = application:stop(pgmp).


numeric_test(Config) ->
    ct_property_test:quickcheck(
      pgmp_prop_types:prop_numeric(),
      Config).


smallint_test(Config) ->
    ct_property_test:quickcheck(
      pgmp_prop_types:prop_smallint(),
      Config).


integer_test(Config) ->
    ct_property_test:quickcheck(
      pgmp_prop_types:prop_integer(),
      Config).


bigint_test(Config) ->
    ct_property_test:quickcheck(
      pgmp_prop_types:prop_bigint(),
      Config).


real_test(Config) ->
    ct_property_test:quickcheck(
      pgmp_prop_types:prop_real(),
      Config).


double_test(Config) ->
    ct_property_test:quickcheck(
      pgmp_prop_types:prop_double(),
      Config).
