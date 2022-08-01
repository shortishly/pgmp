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


-module(pgmp_replication_sup).


-behaviour(supervisor).
-export([init/1]).
-export([start_link/1]).
-import(pgmp_sup, [supervisor/1]).


start_link(#{} = Arg) ->
    supervisor:start_link(?MODULE, [Arg]).


init([Arg]) ->
    case pgmp_config:enabled(pgmp_replication) of
        true ->
            {ok, configuration(children(Arg))};

        false ->
            ignore
    end.


configuration(Children) ->
    {#{intensity => length(Children),
       auto_shutdown => all_significant},
     Children}.


children(Arg) ->
    [supervisor(#{m => pgmp_replication_logical_sup,
                  restart => transient,
                  significant => true,
                  args => [Arg]})].
