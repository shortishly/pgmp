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
-export([start_child/1]).
-export([start_link/1]).
-export([terminate_child/1]).
-import(pgmp_sup, [supervisor/1]).


start_link(#{} = Arg) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [Arg]).

start_child(Pub) ->
    Arg = #{config := Config} = pgmp_sup:config(),
    supervisor:start_child(
      ?MODULE,
      supervisor(#{id => Pub,
                   m => pgmp_rep_log_sup,
                   restart => transient,
                   significant => true,
                   args => [Arg#{config := Config#{publication => Pub}}]})).


terminate_child(Pub) ->
    supervisor:terminate_child(?MODULE, Pub).


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
    {maps:merge(
       #{auto_shutdown => all_significant},
       pgmp_config:sup_flags(?MODULE)),
     Children}.


children(#{config := Config} = Arg) ->
    lists:map(
      fun
          (Pub) ->
              supervisor(
                #{id => Pub,
                  m => pgmp_rep_log_sup,
                  restart => transient,
                  significant => true,
                  args => [Arg#{config := Config#{publication => Pub}}]})
      end,
      pgmp_config:replication(logical, publication_names)).
