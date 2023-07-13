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


-module(pgmp_dbs_sup).


-behaviour(supervisor).
-export([default/1]).
-export([init/1]).
-export([start_child/1]).
-export([start_link/0]).
-export_type([db/0]).
-import(pgmp_sup, [supervisor/1]).


-type db() :: #{user := binary(),
                password => fun (() -> binary()),
                port := binary(),
                host := binary(),
                scope => atom(),
                group => atom(),
                application_name => binary(),
                database := binary()}.


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


start_child(URI) when is_list(URI) ->
    ?FUNCTION_NAME(list_to_binary(URI));

start_child(URI) when is_binary(URI) ->
    supervisor:start_child(
      ?MODULE,
      child_spec(maps:merge(default(), pgmp_uri:parse(URI)))).


init([]) ->
    {ok, {pgmp_config:sup_flags(?MODULE), children()}}.


children() ->
    [child_spec(DB) || DB <- dbs(), pgmp_config:enabled(connect)].


child_spec(#{application_name := ApplicationName} = DB) ->
    supervisor(
       #{id => ApplicationName,
         m => pgmp_db_sup,
         args => [DB]}).


-spec dbs() -> [db()].

dbs() ->
    try
        lists:map(
          fun
              (URI) ->
                  maps:merge(default(), pgmp_uri:parse(URI))
          end,
          string:split(
            pgmp_config:database(uri),
            pgmp_config:database(uri_separator),
            all))

    catch
        error:badarg ->
            [default()]
    end.


default() ->
    lists:foldl(
      fun
          (Parameter, A) ->
              A#{Parameter => default(Parameter)}
      end,
      #{},
      [user,
       application_name,
       password,
       port,
       host,
       database]).


default(password = Parameter) ->
    fun
        () ->
            pgmp_config:database(Parameter)
    end;

default(host) ->
    pgmp_config:database(hostname);

default(database) ->
    pgmp_config:database(name);

default(Parameter) ->
    pgmp_config:database(Parameter).
