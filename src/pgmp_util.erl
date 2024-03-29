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


-module(pgmp_util).


-export([is_exported/3]).
-export([semantic_version/1]).
-export([snake_case/1]).
-export([split_on_snake_case/1]).
-export([tl_snake_case/1]).


snake_case([_ | _] = Labels) ->
    list_to_atom(lists:concat(lists:join("_", Labels))).


split_on_snake_case(Name) ->
    string:split(atom_to_list(Name), "_", all).

tl_snake_case(Name) ->
    case split_on_snake_case(Name) of
        [_] ->
            Name;

        Names ->
            snake_case(tl(Names))
    end.


is_exported(M, F, A) ->
    _ = case erlang:module_loaded(M) of
            false ->
                code:ensure_loaded(M);

            true ->
                ok
        end,
    erlang:function_exported(M, F, A).


semantic_version(Version) ->
    {ok, MP} = re:compile(
                 "(?<major>\\d+)(\\.(?<minor>\\d+)(\\.(?<patch>\\d+))?)?"),
    {namelist, NL} = re:inspect(MP, namelist),
    {match, Matches} = re:run(Version, MP,  [{capture, all_names, binary}]),
    lists:foldl(
      fun
          ({_, <<>>}, A) ->
              A;
          ({K, V}, A) ->
              A#{binary_to_atom(K) => binary_to_integer(V)}
      end,
      #{},
      lists:zip(NL, Matches)).
