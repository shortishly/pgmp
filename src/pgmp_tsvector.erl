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


-module(pgmp_tsvector).

-export([decode/1]).
-export([encode/1]).


decode(<<N:32, Encoded/bytes>>) ->
    {<<>>, Decoded} = pgmp_binary:repeat(
                        N,
                        Encoded,
                        fun words/2,
                        #{}),
    Decoded.

words(WordPositions, A) ->
    position(binary:split(WordPositions, <<0>>), A).

position([Word, <<0:16, Remainder/bytes>>], A) ->
    {Remainder, A#{Word => #{}}};

position([Word, <<NumPos:16, Encoded/bytes>>], A) ->
    {Remainder, Positions} = pgmp_binary:repeat(
                               NumPos,
                               Encoded,
                               fun position_weight/2,
                               #{}),
    {Remainder, A#{Word => Positions}}.

position_weight(<<Weight:2, Position:14, Remainder/bytes>>, A) ->
    {Remainder, A#{Position => weight(Weight)}}.


weight(0) ->
    d;
weight(1) ->
    c;
weight(2) ->
    b;
weight(3) ->
    a;
weight(a) ->
    3;
weight(b) ->
    2;
weight(c) ->
    1;
weight(d) ->
    0.

encode(Value) ->
    [<<(map_size(Value)):32>>,

     lists:map(
       fun
           ({Word, Positions}) ->
               [<<Word/bytes,
                  0:8,
                  (map_size(Positions)):16>>,

                lists:map(
                  fun
                      ({Position, Weight}) ->
                          <<(weight(Weight)):2, Position:14>>
                  end,
                  as_sorted_list(Positions))]
       end,
       as_sorted_list(Value))].


as_sorted_list(M) ->
    lists:sort(maps:to_list(M)).
