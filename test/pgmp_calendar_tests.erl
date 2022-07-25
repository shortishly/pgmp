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


-module(pgmp_calendar_tests).


-include_lib("eunit/include/eunit.hrl").


decode_test() ->
    MicroSincePGEpoch = 711904983599074,
    ?assertEqual(
       {{2022, 7, 23}, {15, 23, 3}},
       calendar:system_time_to_universal_time(
         pgmp_calendar:decode(MicroSincePGEpoch),
         microsecond)).


encode_decode_test() ->
    MicroSincePGEpoch = 711904983599074,
    ?assertEqual(
       MicroSincePGEpoch,
       pgmp_calendar:encode(
         pgmp_calendar:decode(MicroSincePGEpoch))).
