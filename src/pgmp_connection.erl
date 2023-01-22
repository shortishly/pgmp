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


-module(pgmp_connection).


-export([bind/1]).
-export([callback_mode/0]).
-export([describe/1]).
-export([execute/1]).
-export([handle_event/4]).
-export([init/1]).
-export([join/1]).
-export([parse/1]).
-export([query/1]).
-export([ready_for_query/1]).
-export([start_link/0]).
-export([sync/1]).
-import(pgmp_statem, [nei/1]).
-import(pgmp_statem, [send_request/1]).
-include_lib("kernel/include/logger.hrl").


start_link() ->
    gen_statem:start_link({local, ?MODULE},
                          ?MODULE,
                          [],
                          envy_gen:options(?MODULE)).


query(Arg) ->
    pgmp_mm:?FUNCTION_NAME(Arg#{server_ref => ?MODULE}).


parse(Arg) ->
    pgmp_mm:?FUNCTION_NAME(Arg#{server_ref => ?MODULE}).


sync(Arg) ->
    pgmp_mm:?FUNCTION_NAME(Arg#{server_ref => ?MODULE}).


bind(Arg) ->
    pgmp_mm:?FUNCTION_NAME(Arg#{server_ref => ?MODULE}).


describe(Arg) ->
    pgmp_mm:?FUNCTION_NAME(Arg#{server_ref => ?MODULE}).


execute(Arg) ->
    pgmp_mm:?FUNCTION_NAME(Arg#{server_ref => ?MODULE}).


join(Arg) ->
    send_request(Arg, ?FUNCTION_NAME, [group]).


ready_for_query(Arg) ->
    send_request(Arg, ?FUNCTION_NAME, [state]).


send_request(Arg, Action, Config) ->
    send_request(
      maps:without(
        keys(Config),
        maybe_label(
          Arg#{request => {Action, args(Arg, Config)},
               server_ref => ?MODULE}))).


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


callback_mode() ->
    handle_event_function.


init([]) ->
    {ok,
     drained,
     #{requests => gen_statem:reqids_new(),
       max => pgmp_config:pool(max),
       owners => #{},
       reservations => #{},
       outstanding => #{},
       monitors => #{},
       connections => #{}}}.

handle_event({call, {Owner, _} = From},
             {request, _} = Request,
             _,
             #{owners := Owners,
               outstanding := Outstanding,
               requests := Requests} = Data)
  when is_map_key(Owner, Owners) ->
    #{Owner := Connection} = Owners,
    Inflight = maps:get(Connection, Outstanding, []),
    {keep_state,
     Data#{outstanding := Outstanding#{Connection => [From | Inflight]},
           requests := gen_statem:send_request(
                         Connection,
                         Request,
                         #{module => ?MODULE,
                           connection => Connection,
                           from => From},
                         Requests)}};

handle_event({call, {Owner, _} = From},
             {request, _} = Request,
             available,
             #{connections := Connections,
               monitors := Monitors,
               outstanding := Outstanding,
               requests := Requests,
               reservations := Reservations,
               owners := Owners} = Data) ->

    [Connection | _] = Idle = maps:keys(
                                maps:filter(
                                  connection_state_is(idle),
                                  Connections)),

    NextState = case Idle of
                    [Connection] ->
                        drained;

                    [Connection | _] ->
                        available
                end,

    {next_state,
     NextState,
     Data#{connections := Connections#{Connection := reserved},
           outstanding := Outstanding#{Connection => [From]},
           monitors := Monitors#{Owner => erlang:monitor(process, Owner)},
           requests := gen_statem:send_request(
                         Connection,
                         Request,
                         #{module => ?MODULE,
                           connection => Connection,
                           from => From},
                         Requests),
           reservations := Reservations#{Connection => Owner},
           owners := Owners#{Owner => Connection}}};

handle_event({call, _},
             {request, _},
             drained,
             #{max := Max,
               owners := Owners,
               connections := Connections})
             when map_size(Connections) + map_size(Owners) < Max ->
    {ok, _} = pgmp_pool_sup:start_child(),
    {keep_state_and_data, postpone};

handle_event({call, _}, {request, _}, drained, _) ->
    {keep_state_and_data, postpone};

handle_event({call, {Connection, _} = From},
             {join, [_Group]},
             drained,
             #{connections := Connections} = Data) ->
    _ = erlang:monitor(process, Connection),
    {next_state,
     available,
     Data#{connections := Connections#{Connection => idle}},
     {reply, From, ok}};

handle_event({call, {Connection, _} = From},
             {ready_for_query, [idle]},
             _,
             #{outstanding := Outstanding})
  when is_map_key(Connection, Outstanding) ->
    {keep_state_and_data, {reply, From, ok}};

handle_event({call, {Connection, _} = From},
             {ready_for_query, [idle = State]},
             drained,
             #{connections := Connections,
               reservations := Reservations,
               monitors := Monitors,
               owners := Owners} = Data)
  when is_map_key(Connection, Reservations) ->
    #{Connection := Owner} = Reservations,
    #{Owner := Monitor} = Monitors,
    true = erlang:demonitor(Monitor, [flush]),
    {next_state,
     available,
     Data#{connections := Connections#{Connection => State},
           owners := maps:without([Owner], Owners),
           monitors := maps:without([Owner], Monitors),
           reservations := maps:without([Connection], Reservations)},
     {reply, From, ok}};

handle_event({call, {Connection, _} = From},
             {join,  [_Group]},
             _,
             #{connections := Connections} = Data) ->
    _ = erlang:monitor(process, Connection),
    {keep_state,
     Data#{connections := Connections#{Connection => idle}},
     {reply, From, ok}};

handle_event({call, {Connection, _} = From},
             {ready_for_query,  [idle = State]},
             _,
             #{connections := Connections,
               reservations := Reservations,
               monitors := Monitors,
               owners := Owners} = Data)
  when is_map_key(Connection, Reservations) ->
    #{Connection := Owner} = Reservations,
    #{Owner := Monitor} = Monitors,
    true = erlang:demonitor(Monitor, [flush]),
    {keep_state,
     Data#{connections := Connections#{Connection => State},
           owners := maps:without([Owner], Owners),
           monitors := maps:without([Owner], Monitors),
           reservations := maps:without([Connection], Reservations)},
     {reply, From, ok}};

handle_event({call, {Connection, _} = From},
             {ready_for_query,  [State]},
             _,
             #{connections := Connections} = Data) ->
    {keep_state,
     Data#{connections := Connections#{Connection => State}},
     {reply, From, ok}};


handle_event(internal,
             {response,
              #{label := #{module := ?MODULE,
                           connection := Connection,
                           from := {'DOWN', _Monitor, process, Owner, _Info}},
                reply := ok}},
             _,
             #{owners := Owners,
               reservations := Reservations,
               monitors := Monitors} = Data)
  when is_map_key(Owner, Owners) ->
    #{Owner := Connection} = Owners,
    {keep_state,
     Data#{monitors := maps:without([Owner], Monitors),
           reservations := maps:without([Connection], Reservations),
           owners := maps:without([Owner], Owners)}};

handle_event(internal,
             {response,
              #{label := #{module := ?MODULE,
                           connection := Connection,
                           from := {'DOWN', _Monitor, process, Owner, _Info}},
                reply := ok}},
             _,
             #{monitors := Monitors,
               reservations := Reservations} = Data) ->
    {keep_state,
     Data#{monitors := maps:without([Owner], Monitors),
           reservations := maps:without([Connection], Reservations)}};

handle_event(internal,
             {response,
              #{label := #{module := ?MODULE,
                           connection := Connection,
                           from := {'DOWN', _Monitor, process, Owner, _Info}},
                reply := {ready_for_query, in_tx_block}}},
             _,
             #{outstanding := Outstanding,
               owners := Owners,
               reservations := Reservations,
               connections := Connections} = Data)
  when not(is_map_key(Connection, Outstanding)),
       is_map_key(Connection, Connections),
       is_map_key(Connection, Reservations),
       is_map_key(Owner, Owners) ->
    ok = gen_statem:stop(Connection),
    {keep_state,
     Data#{owners := maps:without([Owner], Owners),
           reservations := maps:without([Connection], Reservations),
           connections := Connections#{Connection => limbo}}};

handle_event(internal,
             {response,
              #{label := #{module := ?MODULE,
                           connection := Connection,
                           from := From},
                reply := Reply}},
             _,
             #{outstanding := Outstanding} = Data) ->
    #{Connection := Inflight} = Outstanding,
    case lists:delete(From, Inflight) of
        [] ->
            {keep_state,
             Data#{outstanding := maps:without([Connection], Outstanding)},
             {reply, From, Reply}};

        Remaining ->
            {keep_state,
             Data#{outstanding := Outstanding#{Connection := Remaining}},
             {reply, From, Reply}}
    end;

handle_event(info,
             {'DOWN', _Monitor, process, Owner, _Info},
             _,
             #{monitors := Monitors,
               reservations := Reservations,
               connections := Connections,
               owners := Owners} = Data)
  when is_map_key(Owner, Monitors),
       is_map_key(Owner, Owners) ->
    #{Owner := Connection} = Owners,
    ok = gen_statem:stop(Connection),
    {keep_state,
     Data#{monitors := maps:without([Owner], Monitors),
           owners := maps:without([Owner], Owners),
           reservations := maps:without([Connection], Reservations),
           connections := Connections#{Connection => limbo}}};

handle_event(info,
             {'DOWN', _Monitor, process, Owner, _Info},
             _,
             #{monitors := Monitors} = Data)
  when is_map_key(Owner, Monitors) ->
    {keep_state, Data#{monitors := maps:without([Owner], Monitors)}};

handle_event(info,
             {'DOWN', _Monitor, process, Connection, _Info},
             available,
             #{connections := Connections,
               reservations := Reservations,
               monitors := Monitors,
               owners := Owners} = Data)
  when is_map_key(Connection, Connections),
       is_map_key(Connection, Reservations),
       map_size(Connections) > 1 ->
    #{Connection := Owner} = Reservations,
    #{Owner := Monitor} = Monitors,
    true = erlang:demonitor(Monitor, [flush]),
    {keep_state,
     Data#{connections := maps:without([Connection], Connections),
           owners := maps:without([Owner], Owners),
           monitors := maps:without([Owner], Monitors),
           reservations := maps:without([Connection], Reservations)}};

handle_event(info,
             {'DOWN', _Monitor, process, Connection, _Info},
             _,
             #{connections := Connections,
               reservations := Reservations,
               monitors := Monitors,
               owners := Owners} = Data)
  when is_map_key(Connection, Connections),
       is_map_key(Connection, Reservations) ->
    #{Connection := Owner} = Reservations,
    #{Owner := Monitor} = Monitors,
    true = erlang:demonitor(Monitor, [flush]),
    {next_state,
     drained,
     Data#{connections := maps:without([Connection], Connections),
           owners := maps:without([Owner], Owners),
           monitors := maps:without([Owner], Monitors),
           reservations := maps:without([Connection], Reservations)}};

handle_event(info,
             {'DOWN', _Monitor, process, Connection, _Info},
             available,
             #{connections := Connections} = Data)
  when is_map_key(Connection, Connections),
       map_size(Connections) > 1 ->
    {keep_state,
     Data#{connections := maps:without([Connection], Connections)}};

handle_event(info,
             {'DOWN', _Monitor, process, Connection, _Info},
             _,
             #{connections := Connections} = Data)
  when is_map_key(Connection, Connections) ->
    {next_state,
     drained,
     Data#{connections := maps:without([Connection], Connections)}};

handle_event(info, {'DOWN', _, process, _, _} = Down, State, Data) ->
    ?LOG_ERROR(#{down => Down, state => State, data => Data}),
    keep_state_and_data;

handle_event(info, Msg, _, #{requests := Existing} = Data) ->
    case gen_statem:check_response(Msg, Existing, true) of
        {{reply, Reply}, Label, Updated} ->
            {keep_state,
             Data#{requests := Updated},
             nei({response, #{label => Label, reply => Reply}})};

        {{error, {Reason, _}}, pgmp_socket, UpdatedRequests}
          when Reason == noproc; Reason == normal ->
            {stop,
             normal,
             Data#{requests := UpdatedRequests}};

        {{error, {normal, _}}, #{module := ?MODULE}, UpdatedRequests} ->
            {stop,
             normal,
             Data#{requests := UpdatedRequests}};

        {{error, {Reason, ServerRef}}, Label, UpdatedRequests} ->
                {stop,
                 #{reason => Reason,
                   server_ref => ServerRef,
                   label => Label},
                 Data#{requests := UpdatedRequests}};

        Otherwise ->
            ?LOG_ERROR(#{reason => Otherwise, msg => Msg, data => Data}),
            keep_state_and_data
    end.


connection_state_is(Desired) ->
    fun
        (_, Current) ->
            Current == Desired
    end.
