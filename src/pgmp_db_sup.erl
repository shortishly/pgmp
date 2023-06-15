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


-module(pgmp_db_sup).


-behaviour(supervisor).
-export([init/1]).
-export([start_link/1]).
-import(pgmp_sup, [supervisor/1]).
-import(pgmp_sup, [worker/1]).


start_link(Arg) ->
    supervisor:start_link(?MODULE, [Arg]).


init([Arg]) ->
    {ok, configuration(children(scope(Arg)))}.


configuration(Children) ->
    {maps:merge(
       #{strategy => one_for_all},
       pgmp_config:sup_flags(?MODULE)),
     Children}.

scope(#{application_name := ApplicationName} = Arg) ->
    Arg#{scope => pgmp_util:snake_case(
              [binary_to_list(ApplicationName),
               pgmp_config:pg(scope)])}.

children(#{scope := Scope} = Arg) ->
    [worker(#{m => pg, args => [Scope]}),
     supervisor(#{m => pgmp_int_sup, args => [Arg]}),
     supervisor(#{m => pgmp_rep_sup, args => [Arg]}),
     worker(#{m => pgmp_db, args => [Arg]})].
