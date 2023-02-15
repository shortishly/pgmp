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


-module(pgmp_config).


-export([backoff/1]).
-export([codec/1]).
-export([database/1]).
-export([enabled/1]).
-export([pg/1]).
-export([pool/1]).
-export([protocol/1]).
-export([rep_log_ets/1]).
-export([replication/2]).
-export([sup_flags/1]).
-export([telemetry/1]).
-export([timeout/1]).
-import(envy, [envy/1]).


sup_flags(Supervisor) ->
    lists:foldl(
      fun
          (Name, A) ->
              A#{Name => restart(Supervisor, Name)}
      end,
      #{},
      [intensity, period]).


restart(Supervisor, Name) when Name == intensity; Name == period ->
    envy(#{caller => ?MODULE,
           names => [Supervisor, ?FUNCTION_NAME, Name],
           default => restart(Name)}).

restart(intensity) ->
    1;
restart(period) ->
    5.


backoff(rand_increment = Name) ->
    envy(#{caller => ?MODULE,
           names => [?FUNCTION_NAME, Name],
           default => 1}).

timeout(Name) ->
    envy(#{caller => ?MODULE,
           names => [Name, ?FUNCTION_NAME],
           default => infinity}).


enabled(rep_log_ets_schema_in_table_name = Name) ->
    envy(#{caller => ?MODULE,
           names => [Name, ?FUNCTION_NAME],
           default => false});

enabled(rep_log_ets_pub_in_table_name = Name) ->
    envy(#{caller => ?MODULE,
           names => [Name, ?FUNCTION_NAME],
           default => false});

enabled(Name) ->
    envy(#{caller => ?MODULE,
           names => [Name, ?FUNCTION_NAME],
           default => true}).


codec(Type) ->
    envy(#{caller => ?MODULE,
           names => [?FUNCTION_NAME, Type],
           default => pgmp_identity}).


pg(scope = Name) ->
    envy(#{caller => ?MODULE,
           names => [?FUNCTION_NAME, Name],
           default => pgmp});

pg(group = Name) ->
    envy(#{caller => ?MODULE,
           names => [?FUNCTION_NAME, Name],
           default => mm}).


protocol(major = Name) ->
    envy(#{caller => ?MODULE,
           names => [?FUNCTION_NAME, Name],
           default => 3});

protocol(minor = Name) ->
    envy(#{caller => ?MODULE,
           names => [?FUNCTION_NAME, Name],
           default => 0}).

rep_log_ets(prefix_table_name = Name) ->
    envy(#{caller => ?MODULE,
           names => [?FUNCTION_NAME, Name],
           default => []}).

replication(logical = Type, module = Name) ->
    envy(#{caller => ?MODULE,
           names => [?FUNCTION_NAME, Type, Name],
           default => pgmp_rep_log_ets});

replication(logical = Type, slot_prefix = Name) ->
    envy(#{caller => ?MODULE,
           names => [?FUNCTION_NAME, Type, Name],
           default => <<"pgmp">>});

replication(logical = Type, max_rows = Name) ->
    envy(#{caller => ?MODULE,
           names => [?FUNCTION_NAME, Type, Name],
           default => 5_000});

replication(logical = Type, publication_names = Name) ->
      binary:split(
        envy(#{caller => ?MODULE,
               names => [?FUNCTION_NAME, Type, Name],
               default => <<"pub">>}),
        <<",">>,
        [global, trim_all]).


database(options = Name) ->
    envy(#{type => list,
           caller => ?MODULE,
           names => [?FUNCTION_NAME, Name]});

database(hostname = Name) ->
    envy(#{caller => ?MODULE,
           names => [?FUNCTION_NAME, Name],
           default => "localhost"});

database(port = Name) ->
    envy(#{caller => ?MODULE,
           names => [?FUNCTION_NAME, Name],
           default => 5432});

database(user = Name) ->
    envy(#{type => binary,
           caller => ?MODULE,
           names => [?FUNCTION_NAME, Name],
           default => os:getenv("USER")});

database(password = Name) ->
    envy(#{caller => ?MODULE,
           names => [?FUNCTION_NAME, Name],
           default => <<"">>});

database(replication = Name) ->
    envy(#{caller => ?MODULE,
           names => [?FUNCTION_NAME, Name],
           default => <<"false">>});

database(name = Name) ->
    envy(#{caller => ?MODULE,
           names => [?FUNCTION_NAME, Name],
           default => ?FUNCTION_NAME(user)}).


pool(max = Name) ->
    envy(#{caller => ?MODULE,
           names => [?FUNCTION_NAME, Name],
           default => 5}).


telemetry(Name) when Name == module; Name == function ->
    envy(#{caller => ?MODULE,
           type => atom,
           names =>[?FUNCTION_NAME, Name]});

telemetry(event_names = Name) ->
    envy(#{caller => ?MODULE,
           names => [?FUNCTION_NAME, Name],
           default => filename:join(pgmp:priv_dir(), "telemetry.terms")});

telemetry(config = Name) ->
    envy:get_env(pgmp,
                 pgmp_util:snake_case([?FUNCTION_NAME, Name]),
                 [app_env, {default, []}]).
