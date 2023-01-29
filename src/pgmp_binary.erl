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


-module(pgmp_binary).


-export([foldl/3]).
-export([repeat/4]).

-type mapping() :: fun((binary(), any()) -> {binary(), any()}).


-spec foldl(mapping(), any(), binary()) -> any().

foldl(_, A, <<>>) ->
    A;

foldl(F, A0, Binary) ->
    {Remainder, A1} = F(Binary, A0),
    ?FUNCTION_NAME(F, A1, Remainder).


-spec repeat(non_neg_integer(), binary(), mapping(), any()) -> {binary(), any()}.

repeat(0, Data, _, A) ->
    {Data, A};

repeat(N, Data, F, A0) ->
    {Remainder, A1} = F(Data, A0),
    ?FUNCTION_NAME(N - 1, Remainder, F, A1).
