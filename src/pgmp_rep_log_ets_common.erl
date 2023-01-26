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


-module(pgmp_rep_log_ets_common).


-export([insert_new/3]).
-export([insert_or_update_tuple/2]).
-export([metadata/4]).
-export([update/3]).


metadata(Table, Key, Value, Metadata) ->
    case Metadata of
        #{Table := TMD} ->
            Metadata#{Table := TMD#{Key => Value}};

        #{} ->
            Metadata#{Table => #{Key => Value}}
    end.


insert_new(Relation, Tuple, Keys) ->
    ets:insert_new(
      binary_to_atom(Relation),
      insert_or_update_tuple(Tuple, Keys)).


update(Relation, Tuple, Keys) ->
    ets:insert(
      binary_to_atom(Relation),
      insert_or_update_tuple(Tuple, Keys)).


insert_or_update_tuple(Tuple, [_]) ->
    Tuple;

insert_or_update_tuple(Tuple, Composite) ->
    list_to_tuple(
      [list_to_tuple([element(Pos, Tuple) || Pos <- Composite]) |
       lists:filtermap(
         fun
             ({Position, Value}) ->
                 case lists:member(Position, Composite) of
                     true ->
                         false;

                     false ->
                         {true, Value}
                 end
         end,
         lists:zip(
           lists:seq(1, tuple_size(Tuple)),
           tuple_to_list(Tuple)))]).
