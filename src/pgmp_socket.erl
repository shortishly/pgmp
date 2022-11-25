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
    {ok, disconnected, Arg#{requests => gen_statem:reqids_new()}}.


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

handle_event(internal, open, _, Data) ->
    case socket:open(inet, stream, default) of
        {ok, Socket} ->
            {keep_state, Data#{socket => Socket}, nei(connect)};

        {error, Reason} ->
            {stop, Reason}
    end;

handle_event(internal, {send, Data}, _, #{socket := Socket}) ->
    case socket:send(Socket, Data) of
        ok ->
            keep_state_and_data;

        {error, Reason} ->
            {stop, Reason}
    end;

handle_event(internal, recv, _, #{socket := Socket, partial := Partial} = Data) ->
    case socket:recv(Socket, 0, nowait) of
        {ok, Received} ->
            {keep_state,
             Data#{partial := <<>>},
             [nei({recv, iolist_to_binary([Partial, Received])}), nei(recv)]};

        {select, {select_info, _, _}} ->
            keep_state_and_data;

        {error, Reason} ->
            {stop, Reason}
    end;

handle_event(info,
             {'$socket', Socket, select, Handle},
             _,
             #{socket := Socket, partial := Partial} = Data) ->
    case socket:recv(Socket, 0, Handle) of
        {ok, Received} ->
            {keep_state,
             Data#{partial := <<>>},
             [nei({recv, iolist_to_binary([Partial, Received])}), nei(recv)]};

        {select, {select_info, _, _}} ->
            keep_state_and_data;

        {error, Reason} ->
            {stop, Reason}
    end;

handle_event(info, Msg, _, #{requests := Existing} = Data) ->
    case gen_statem:check_response(Msg, Existing, true) of
        {{reply, Reply}, Label, Updated} ->
            {keep_state,
             Data#{requests := Updated},
             nei({response, #{label => Label, reply => Reply}})};

        {{error, {Reason, ServerRef}}, Label, UpdatedRequests} ->
                {stop,
                 #{reason => Reason,
                   server_ref => ServerRef,
                   label => Label},
                 Data#{requests := UpdatedRequests}}
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
             {tag_msg, Tag, Message},
             _,
             #{peer := Peer, requests := Requests} = Data) ->
    {keep_state,
     Data#{requests := pgmp_mm:recv(
                         #{server_ref => Peer,
                           tag => pgmp_message_tags:name(backend, Tag),
                           message => Message,
                           requests => Requests})}};

handle_event(internal, connect, _, #{socket := Socket} = Data) ->
    case socket:connect(
           Socket,
           #{family => inet,
             port => pgmp_config:database(port),
             addr => addr()}) of

        ok ->
            {next_state, connected, Data#{partial => <<>>}, nei(recv)};

        {error, Reason} ->
            {stop, Reason}
    end.


addr() ->
    ?FUNCTION_NAME(pgmp_config:database(hostname)).


addr(Hostname) ->
    {ok, #hostent{h_addr_list = Addresses}} = inet:gethostbyname(Hostname),
    pick_one(Addresses).


pick_one(L) ->
    lists:nth(rand:uniform(length(L)), L).
