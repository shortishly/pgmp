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


-module(pgmp_mm_bootstrap).


-export([callback_mode/0]).
-export([handle_event/4]).
-export([terminate/3]).
-import(pgmp_codec, [demarshal/1]).
-import(pgmp_codec, [marshal/2]).
-import(pgmp_codec, [prefix_with_size/1]).
-import(pgmp_data_row, [decode/2]).
-import(pgmp_statem, [nei/1]).
-include_lib("kernel/include/logger.hrl").


callback_mode() ->
    [handle_event_function, state_enter].


terminate(Reason, State, Data) ->
    pgmp_mm_common:terminate(Reason, State, Data).


handle_event({call, _}, {request, _}, unready, Data) ->
    {next_state, starting, Data, [postpone, nei(startup)]};

handle_event({call, From},
             {request, _},
             startup_failure,
             #{errors := Errors}) ->
    {keep_state_and_data, {reply, From, [Errors]}};

handle_event({call, _}, {request, _}, _, _) ->
    {keep_state_and_data, postpone};

handle_event(internal,
             peer,
             _,
             #{supervisor := Supervisor} = Data) ->
    case pgmp_sup:get_child(Supervisor, socket) of
        {_, PID, worker, _} when is_pid(PID) ->
            {keep_state,
             Data#{socket => PID},
             nei(eager_startup_on_replication)};

        {_, _, _, _} = Reason ->
            {stop, Reason};

        false ->
            {stop, peer_not_found}
    end;

handle_event(internal, join, _, #{config := #{group := Group}}) ->
    pgmp_pg:join(Group),
    keep_state_and_data;

handle_event(internal, join, _, #{config := #{}}) ->
    keep_state_and_data;

handle_event(internal,
             {response, #{label := pgmp_types, reply := ready}},
             _,
             #{type_catalogue := false} = Data) ->
    {keep_state, maps:without([type_catalogue], Data)};

handle_event(internal, types_when_ready, _, Data) ->
    {keep_state,
     Data#{requests := pgmp_types:when_ready(
                         maps:with([requests], Data))}};

handle_event(internal,
             eager_startup_on_replication,
             _,
             #{config := #{replication := Replication}}) ->
    {keep_state_and_data,
     nei({eager_startup_for_replication, Replication()})};

handle_event(internal,
             {eager_startup_for_replication, <<"false">>},
             _,
             _) ->
    keep_state_and_data;

handle_event(internal,
             {eager_startup_for_replication, _},
             _,
             _) ->
    {keep_state_and_data, nei(startup)};

handle_event(internal, startup, _, _) ->
    {keep_state_and_data, nei({startup, #{}})};

handle_event(internal,
             {startup, KV},
             _,
             #{config := #{identity := #{user := User},
                           database := Database,
                           replication := Replication}}) ->
    {keep_state_and_data,
     nei({send,
          prefix_with_size(
            [marshal({int, 32}, version()),
             maps:fold(
               fun
                   (K, V, A) ->
                       [marshal(string, K), marshal(string, V) | A]
               end,
               [],
               maps:merge(
                 #{<<"user">> => User(),
                   <<"database">> => Database(),
                   <<"replication">> => Replication()},
                 KV)),
             marshal(byte, <<0>>)])})};

handle_event(internal, {recv, {authentication, authenticated}}, State, Data)
  when State == unready; State == starting ->
    {next_state, authenticated, Data};

handle_event(internal, {recv, {error_response, Reason}}, unready, _) ->
    {stop, Reason};

handle_event(internal,
             {recv, {ready_for_query, _} = TM},
             authenticated,
             #{config := #{replication := Replication}} = Data) ->
    {next_state, TM, Data, nei({replication, Replication()})};

handle_event(internal, {replication, Type}, _, _) ->
    {keep_state_and_data,
     [{change_callback_module, bootstrap_complete_callback_module(Type)},
      nei(bootstrap_complete)]};

handle_event(internal,
             {recv, {authentication, {sasl, _} = SASL}},
             _,
             _) ->
    {keep_state_and_data,
     [{push_callback_module, pgmp_mm_auth_sasl}, nei(SASL)]};

handle_event(internal,
             {recv, {authentication, {md5_password, _} = MD5}},
             _,
             _) ->
    {keep_state_and_data,
     [{push_callback_module, pgmp_mm_auth_md5}, nei(MD5)]};

handle_event(internal,
             {recv, {backend_key_data, [PID, Key]}},
             _,
             Data) ->
    {keep_state, Data#{backend => #{pid => PID, key => Key}}};

handle_event(EventType, EventContent, State, Data) ->
    pgmp_mm_common:handle_event(EventType,
                                EventContent,
                                State,
                                Data).


version() ->
    <<Version:32>> = <<(pgmp_config:protocol(major)):16,
                       (pgmp_config:protocol(minor)):16>>,
    Version.


bootstrap_complete_callback_module(<<"false">>) ->
    pgmp_mm_simple_query;

bootstrap_complete_callback_module(<<"database">>) ->
    pgmp_mm_replication_logical;

bootstrap_complete_callback_module(Physical) when Physical == <<"true">>;
                                                  Physical == <<"on">>;
                                                  Physical == <<"yes">>;
                                                  Physical == <<"1">> ->
    pgmp_mm_replication_physical.
