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


-export([from/1]).

epoch(pg) ->
    calendar:datetime_to_gregorian_seconds({{2000, 1, 1}, {0, 0, 0}}).


from(<<PG:64>>) ->
    calendar:gregorian_seconds_to_datetime((PG div 1_000_000) + epoch(pg)).
