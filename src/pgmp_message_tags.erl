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


-module(pgmp_message_tags).


-export([callback_mode/0]).
-export([init/1]).
-export([name/2]).
-export([start/0]).
-export([start_link/0]).
-include_lib("stdlib/include/ms_transform.hrl").


start_link() ->
    gen_statem:start_link(?MODULE, [], envy_gen:options(?MODULE)).


start() ->
    gen_statem:start(?MODULE, [], []).

name(Role, Tag) ->
    case ets:lookup(?MODULE, {Role, Tag}) of
        [{_, Name}] ->
            Name;

        [] ->
            error(badarg, [Role, Tag])
    end.


init([]) ->
    process_flag(trap_exit, true),
     ets:insert_new(
       ets:new(?MODULE, [named_table]),
       lists:flatmap(
         fun
             ({Name, {both, Tag}}) ->
                 [{{frontend, Tag}, Name},
                  {{backend, Tag}, Name}];

             ({Name, {_Role, _Tag} = RoleTag}) ->
                 [{RoleTag, Name}]
         end,
         pgmp:priv_consult("message-tags.terms"))),
    {ok, ready, #{}}.


callback_mode() ->
    handle_event_function.
