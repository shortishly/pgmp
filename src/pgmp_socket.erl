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


-module(pgmp_socket).


-export([callback_mode/0]).
-export([handle_event/4]).
-export([init/1]).
-export([send/1]).
-export([start_link/1]).
-export([terminate/3]).
-import(pgmp_statem, [nei/1]).
-include_lib("kernel/include/inet.hrl").
-include_lib("kernel/include/logger.hrl").


start_link(Arg) ->
    gen_statem:start_link(?MODULE, [Arg], envy_gen:options(?MODULE)).


send(#{data := Data} = Arg) ->
    send_request(
      maps:without(
        [data],
        Arg#{request => {?FUNCTION_NAME, Data}})).

send_request(#{label := _} = Arg) ->
    pgmp_statem:send_request(Arg);

send_request(Arg) ->
    pgmp_statem:send_request(Arg#{label => ?MODULE}).


init([Arg]) ->
    process_flag(trap_exit, true),
    {ok,
     disconnected,
     #{requests => gen_statem:reqids_new(),
       config => Arg,
       telemetry => #{db => #{system => postgresql}}}}.


callback_mode() ->
    handle_event_function.


handle_event({call, {Peer, _} = From},
             {send, Message},
             connected,
             #{peer := Peer}) ->
    {keep_state_and_data,
     [nei({send, Message}), {reply, From, ok}]};

handle_event({call, {Peer, _}}, {send, _}, _, Data) ->
    {keep_state,
     Data#{peer => Peer},
     [postpone, nei(open)]};

handle_event(internal,
             {telemetry, EventName, Measurements},
             _,
             _) ->
    {keep_state_and_data,
     nei({telemetry, EventName, Measurements, #{}})};

handle_event(internal,
             {telemetry, EventName, Measurements, Metadata},
             _,
             Data) ->
    ok = telemetry:execute(
           [pgmp, socket, EventName],
           Measurements,
           maps:merge(
             maps:with([peer, socket, telemetry], Data),
             Metadata)),
    keep_state_and_data;

handle_event(internal,
             {error, #{event := EventName,  reason := Reason}},
             _,
             Data) ->
    {next_state,
     limbo,
     Data,
     [nei({telemetry,
           error,
           #{count => 1},
           #{event => EventName, reason => Reason}}),

      {state_timeout,
       timer:seconds(
         backoff:rand_increment(
           pgmp_config:backoff(rand_increment))),
       {backoff, #{action => EventName, reason => Reason}}}]};

handle_event(internal, open = EventName, _, Data) ->
    case socket:open(inet, stream, default) of
        {ok, Socket} ->
            {keep_state,
             Data#{socket => Socket},
             [nei({telemetry, EventName, #{count => 1}}),
              nei(connect)]};

        {error, Reason} ->
            {keep_state_and_data,
             nei({error, #{reason => Reason, event => EventName}})}
    end;

handle_event(internal, {send = EventName, Data}, _, #{socket := Socket}) ->
    case socket:send(Socket, Data) of
        ok ->
            {keep_state_and_data,
             nei({telemetry,
                  EventName,
                  #{bytes => iolist_size(Data)}})};

        {error, Reason} ->
            {keep_state_and_data,
             nei({error, #{reason => Reason, event => EventName}})}
    end;

handle_event(internal,
             recv = EventName,
             _,
             #{socket := Socket, partial := Partial} = Data) ->
    case socket:recv(Socket, 0, nowait) of
        {ok, Received} ->
            {keep_state,
             Data#{partial := <<>>},
             [nei({telemetry, EventName, #{bytes => iolist_size(Received)}}),
              nei({recv, iolist_to_binary([Partial, Received])}),
              nei(recv)]};

        {select, {select_info, _, _}} ->
            keep_state_and_data;

        {error, Reason} ->
            {keep_state_and_data,
             nei({error, #{reason => Reason, event => EventName}})}
    end;

handle_event(info,
             {'$socket', Socket, select, Handle},
             _,
             #{socket := Socket, partial := Partial} = Data) ->
    case socket:recv(Socket, 0, Handle) of
        {ok, Received} ->
            {keep_state,
             Data#{partial := <<>>},
             [nei({telemetry,
                   recv,
                   #{bytes => iolist_size(Received)},
                   #{handle => Handle}}),
              nei({recv, iolist_to_binary([Partial, Received])}),
              nei(recv)]};

        {select, {select_info, _, _}} ->
            keep_state_and_data;

        {error, Reason} ->
            {keep_state_and_data,
             nei({error, #{reason => Reason, event => recv}})}
    end;

handle_event(info, {'DOWN', _, process, Peer, noproc}, _, #{peer := Peer}) ->
    stop;

handle_event(info, Msg, _, #{requests := Existing} = Data) ->
    case gen_statem:check_response(Msg, Existing, true) of
        {{reply, Reply}, Label, Updated} ->
            {keep_state,
             Data#{requests := Updated},
             nei({response, #{label => Label, reply => Reply}})};

        {{error, {Reason, _}}, _, UpdatedRequests} ->
            {stop, Reason, Data#{requests := UpdatedRequests}}
    end;

handle_event(internal, {response, #{label := pgmp_mm, reply :=  ok}}, _, _) ->
    keep_state_and_data;

handle_event(internal,
             {recv,
              <<Tag:1/signed-bytes,
                Length:32/signed,
                Message:(Length - 4)/bytes, Remainder/bytes>>},
             _,
             _) ->
    {keep_state_and_data,
     [nei({tag_msg, Tag, Message}), nei({recv, Remainder})]};

handle_event(internal, {recv, <<>>}, _, _) ->
    keep_state_and_data;

handle_event(internal, {recv, Partial}, _, #{partial := <<>>} = Data) ->
    {keep_state, Data#{partial := Partial}, nei(recv)};

handle_event(internal,
             {tag_msg = EventName, Tag, Message},
             _,
             #{peer := Peer, requests := Requests} = Data) ->
    {keep_state,
     Data#{requests := pgmp_mm:recv(
                         #{server_ref => Peer,
                           tag => pgmp_message_tags:name(backend, Tag),
                           message => Message,
                           requests => Requests})},
     nei({telemetry,
          EventName,
          #{count => 1, bytes => iolist_size(Message)},
          #{tag => pgmp_message_tags:name(backend, Tag)}})};

handle_event(internal,
             connect,
             _,
             #{config := #{host := Host, port := Port}}) ->
    {keep_state_and_data,
     nei({connect,
          #{family => inet,
            port => Port,
            addr => addr(Host)}})};

handle_event(internal,
             {connect = EventName,
              #{family := Family, port := Port, addr := Addr} = SockAddr},
             _,
             #{socket := Socket, telemetry := Telemetry} = Data) ->

    case socket:connect(Socket, SockAddr) of
        ok ->
            {next_state,
             connected,
             Data#{partial => <<>>,
                   telemetry :=
                       Telemetry#{net => #{peer => #{name => Addr,
                                                     port => Port},
                                           sock => #{family => Family}}}},
             [nei({telemetry, EventName, #{count => 1}}),
              nei(recv)]};

        {error, Reason} ->
            {keep_state_and_data,
             nei({error, #{reason => Reason, event => EventName}})}
    end;

handle_event(state_timeout, {backoff, _}, limbo, _) ->
    stop.


terminate(_Reason, _State, #{socket := Socket}) ->
    _ = socket:close(Socket);

terminate(_Reason, _State, _Data) ->
    ok.


addr(Hostname) ->
    case inet:gethostbyname(binary_to_list(Hostname)) of
        {ok, #hostent{h_addr_list = Addresses}} ->
            pick_one(Addresses);

        {error, _} = Error ->
            Error
    end.


pick_one(L) ->
    lists:nth(rand:uniform(length(L)), L).
