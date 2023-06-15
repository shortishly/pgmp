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


-module(pgmp_rep_sup).


-behaviour(supervisor).
-export([init/1]).
-export([start_child/3]).
-export([start_link/1]).
-export([terminate_child/2]).
-import(pgmp_sup, [supervisor/1]).


start_link(Arg) ->
    supervisor:start_link(?MODULE, [Arg]).


start_child(Supervisor, DB, Publication) ->
    supervisor:start_child(
      Supervisor,
      supervisor(#{id => Publication,
                   m => pgmp_rep_log_sup,
                   args => [DB#{publication => Publication}]})).


terminate_child(Sup, Pub) ->
    supervisor:terminate_child(Sup, Pub).


init([Arg]) ->
    {ok,
     configuration(
       case pgmp_config:enabled(pgmp_replication) of
           true ->
               children(Arg);

           false ->
               []
       end)}.


configuration(Children) ->
    {pgmp_config:sup_flags(?MODULE), Children}.


children(Arg) ->
    lists:map(
      fun
          (Pub) ->
              supervisor(
                #{id => Pub,
                  m => pgmp_rep_log_sup,
                  args => [Arg#{publication => Pub}]})
      end,
      pgmp_config:replication(logical, publication_names)).
