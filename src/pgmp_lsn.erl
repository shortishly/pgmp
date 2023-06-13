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


-module(pgmp_lsn).


-export([decode/1]).
-export([encode/1]).


decode(LSN) when is_binary(LSN) ->
    case string:split(LSN, "/", all) of
        [Upper, Lower] ->
            decode(binary_to_integer(Upper, 16),
                   binary_to_integer(Lower, 16));
        _ ->
            error(badarg, [LSN])
    end.

decode(Upper, Lower) ->
    (Upper bsl 32) + Lower.


encode(LSN) when is_integer(LSN) ->
    <<Upper:32, Lower:32>> = <<LSN:64>>,
    iolist_to_binary(
      io_lib:format("~8.16B/~8.16B", [Upper, Lower])).
