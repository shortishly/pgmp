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

-module(pgmp_calendar).


-export([decode/1]).
-export([encode/1]).
-export([epoch/1]).
-export([epoch_date/1]).


%% As microseconds since midnight on 2000-01-01
decode(<<Encoded:64>>) ->
    ?FUNCTION_NAME(Encoded);

decode(MicroSincePGEpoch) ->
      epoch(pg) + MicroSincePGEpoch - epoch(posix).

encode(MicroSystemTime) ->
    epoch(posix) + MicroSystemTime - epoch(pg).


epoch(System) ->
    erlang:convert_time_unit(
      calendar:datetime_to_gregorian_seconds(epoch_datetime(System)),
      second,
      microsecond).


epoch_datetime(System) ->
    {epoch_date(System), midnight()}.


midnight() ->
    {0, 0, 0}.


epoch_date(pg) ->
    {2000, 1, 1};

epoch_date(posix) ->
    {1970, 1, 1}.
