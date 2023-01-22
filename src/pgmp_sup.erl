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


-module(pgmp_sup).


-behaviour(supervisor).
-export([config/0]).
-export([get_child/2]).
-export([init/1]).
-export([start_link/0]).
-export([supervisor/1]).
-export([worker/1]).


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


init([]) ->
    {ok, configuration()}.


configuration() ->
    {pgmp_config:sup_flags(?MODULE), children()}.


children() ->
    [worker(#{m => pg, args => [pgmp_config:pg(scope)]}),
     worker(pgmp_telemetry),
     worker(pgmp_message_tags),
     worker(pgmp_error_notice_fields),
     supervisor(#{m => pgmp_int_sup, args => [config()]}),
     supervisor(#{m => pgmp_rep_sup, args => [config()]})].


config() ->
    #{config => #{identity => identity(), database => database()}}.


database() ->
    config_database(name).


identity() ->
    lists:foldl(
      fun
          (Key, A) ->
              A#{Key => config_database(Key)}
      end,
      #{},
      [user, password]).


config_database(Key) ->
    fun
        () ->
            pgmp_config:database(Key)
    end.


worker(Arg) ->
    child(Arg).


supervisor(Arg) ->
    maps:merge(child(Arg), #{type => supervisor}).


child(#{m := M} = Arg) ->
    maps:merge(
      #{id => pgmp_util:tl_snake_case(M), start => mfargs(Arg)},
      maps:with(keys(), Arg));

child(Arg) when is_atom(Arg) ->
    ?FUNCTION_NAME(#{m => Arg}).


mfargs(#{m := M} = Arg) ->
    {M, maps:get(f, Arg, start_link), maps:get(args, Arg, [])}.


keys() ->
    [id, start, restart, significant, shutdown, type, modules].


get_child(SupRef, Id) ->
    lists:keyfind(Id, 1, supervisor:which_children(SupRef)).
