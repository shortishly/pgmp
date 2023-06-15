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


-module(pgmp_db).


-behaviour(gen_statem).
-export([callback_mode/0]).
-export([config/1]).
-export([handle_event/4]).
-export([init/1]).
-export([start_link/1]).
-export([start_replication_on_publication/2]).
-export([types/1]).
-export([which_groups/1]).


start_link(Arg) ->
    gen_statem:start_link(?MODULE, [Arg], envy_gen:options(?MODULE)).


start_replication_on_publication(Server, Publication) ->
    gen_statem:call(Server, {?FUNCTION_NAME, Publication}).


types(Server) ->
    gen_statem:call(Server, ?FUNCTION_NAME).


config(Server) ->
    gen_statem:call(Server, ?FUNCTION_NAME).


which_groups(Server) ->
    gen_statem:call(Server, ?FUNCTION_NAME).


init([DB]) ->
    {ok, ready, #{supervisor => hd(get('$ancestors')), db => DB}}.


callback_mode() ->
    handle_event_function.


handle_event({call, From},
             {start_replication_on_publication, Publication},
             _,
             #{supervisor := Supervisor, db := DB}) ->
    {keep_state_and_data,
     {reply,
      From,
      pgmp_rep_sup:start_child(
        pgmp_sup:get_child_pid(Supervisor, rep_sup),
        DB,
        Publication)}};

handle_event({call, From}, types, _, #{supervisor := Supervisor}) ->
    {keep_state_and_data,
     {reply,
      From,
      pgmp_sup:get_child_pid(
        pgmp_sup:get_child_pid(Supervisor, int_sup),
        types)}};

handle_event({call, From}, config, _, #{db := Config}) ->
    {keep_state_and_data, {reply, From, Config}};

handle_event({call, From}, which_groups, _, #{db := #{scope := Scope}}) ->
    {keep_state_and_data, {reply, From, pg:which_groups(Scope)}}.
