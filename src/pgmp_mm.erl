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


-module(pgmp_mm).


-export([bind/1]).
-export([callback_mode/0]).
-export([describe/1]).
-export([execute/1]).
-export([init/1]).
-export([parse/1]).
-export([query/1]).
-export([recv/1]).
-export([start_link/1]).
-export([start_replication/1]).
-export([terminate/3]).
-import(pgmp_codec, [demarshal/1]).
-import(pgmp_codec, [marshal/2]).
-import(pgmp_data_row, [decode/2]).
-import(pgmp_statem, [nei/1]).
-import(pgmp_statem, [send_request/1]).
-include_lib("kernel/include/logger.hrl").


start_link(Arg) ->
    gen_statem:start_link(?MODULE, [Arg], pgmp_config:options(?MODULE)).


recv(#{tag := Tag, message := Message} = Arg) ->
    send_request(
      maps:without(
        [tag, message],
        maybe_label(Arg#{request => {?FUNCTION_NAME, {Tag, Message}}}))).


start_replication(Arg) ->
    send_request(Arg, ?FUNCTION_NAME, []).


query(Arg) ->
    send_request(Arg, ?FUNCTION_NAME, [sql]).


parse(Arg) ->
    send_request(Arg, ?FUNCTION_NAME, [{name, <<>>}, sql]).


bind(Arg) ->
    send_request(
      Arg,
      ?FUNCTION_NAME,
      [{name, <<>>}, {portal, <<>>}, {args, []}]).


describe(Arg) ->
    send_request(
      Arg,
      ?FUNCTION_NAME,
      [type, name]).


execute(Arg) ->
    send_request(
      Arg,
      ?FUNCTION_NAME,
      [{portal, <<>>}, {max_rows, 0}]).


send_request(Arg, Action, Config) ->
    send_request(
      maps:without(
        keys(Config),
        maybe_label(
          Arg#{request => {request,
                           #{action => Action,
                             args => args(Arg, Config)}}}))).


maybe_label(#{requests := _, label := _} = Arg) ->
    Arg;

maybe_label(#{requests := _} = Arg) ->
    Arg#{label => ?MODULE};

maybe_label(Arg) ->
    Arg.


keys(Config) ->
    lists:map(
      fun
          ({Key, _}) ->
              Key;

          (Key) ->
              Key
      end,
      Config).


args(Arg, Config) ->
    lists:map(
      fun
          ({Parameter, Default}) ->
              maps:get(Parameter, Arg, Default);

          (Parameter) ->
              maps:get(Parameter, Arg)
      end,
      Config).


init([Arg]) ->
    process_flag(trap_exit, true),
    {ok,
     unready,
     Arg#{cache => ets:new(?MODULE, []),
          requests => gen_statem:reqids_new(),
          types_ready => false,
          parameters => #{}},
     [{change_callback_module, pgmp_mm_bootstrap},
      nei(join),
      nei(types_when_ready),
      nei(peer)]}.


callback_mode() ->
    [handle_event_function, state_enter].


terminate(_Reason, _State, #{config := #{group := Group}}) ->
    pgmp_pg:leave(Group);

terminate(_Reason, _State, _Data) ->
    ok.
