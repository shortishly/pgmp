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


-module(pgmp_tsquery).

-export([decode/1]).
-export([encode/1]).


-define(QI_VAL, 1).
-define(QI_OPR, 2).

-define(OP_NOT, 1).
-define(OP_AND, 2).
-define(OP_OR, 3).
-define(OP_PHRASE, 4).


decode(<<N:32, Encoded/bytes>>) ->
    {<<>>, Decoded} = pgmp_binary:repeat(
                        N,
                        Encoded,
                        fun items/2,
                        []),
    lists:reverse(Decoded).

items(<<?QI_VAL:8, Weight:8, Prefix:8, Encoded/bytes>>, A) ->
    [Operand, Remainder] = binary:split(Encoded, <<0>>),
    {Remainder,
     [#{weight => Weight,
        prefix => prefix(Prefix),
        operand => Operand} | A]};

items(<<?QI_OPR:8, ?OP_NOT:8, Remainder/bytes>>, A) ->
    {Remainder, ['not' | A]};

items(<<?QI_OPR:8, ?OP_AND:8, Remainder/bytes>>, A) ->
    {Remainder, ['and' | A]};

items(<<?QI_OPR:8, ?OP_OR:8, Remainder/bytes>>, A) ->
    {Remainder, ['or' | A]};

items(<<?QI_OPR:8, ?OP_PHRASE:8, Distance:16, Remainder/bytes>>, A) ->
    {Remainder, [{phrase, Distance} | A]}.

prefix(0) ->
    false;
prefix(1) ->
    true;
prefix(false) ->
    0;
prefix(true) ->
    1.


encode(L) ->
    [<<(length(L)):32>>,
     lists:map(
       fun
           (#{weight := Weight, prefix := Prefix, operand := Operand}) ->
               <<?QI_VAL:8,
                 Weight:8,
                 (prefix(Prefix)):8,
                 Operand/bytes,
                 0:8>>;

           ('not') ->
               <<?QI_OPR:8, ?OP_NOT:8>>;

           ('and') ->
               <<?QI_OPR:8, ?OP_AND:8>>;

           ('or') ->
               <<?QI_OPR:8, ?OP_OR:8>>;

           ({phrase, Distance}) ->
               <<?QI_OPR:8, ?OP_PHRASE:8, Distance:16>>
       end,
       L)].
