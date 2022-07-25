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


-module(pgmp_mm_common).


-export([actions/3]).
-export([data/3]).
-export([field_names/1]).
-export([handle_event/4]).
-export([terminate/3]).
-import(pgmp_codec, [demarshal/1]).
-import(pgmp_codec, [prefix_with_size/1]).
-import(pgmp_statem, [nei/1]).


field_names(Types) ->
    [Name || #{field_name := Name} <- Types].


data({call, From}, #{args := Args}, Data) ->
    Data#{from => From, args => Args, replies => []}.


actions({call, _} = Call, Arg, Data) ->
    [command(Call, Arg, Data) | post_actions(Call, Arg, Data)].


command(_, #{action := Action, args := Args}, _) ->
    nei({Action, Args}).


post_actions(_, #{action := Action}, _) ->
    [nei(flush) || Action == parse
                       orelse Action == describe
                       orelse Action == bind
                       orelse Action == execute].


terminate(_Reason, _State, #{config := #{group := Group}}) ->
    pgmp_pg:leave(Group);

terminate(_Reason, _State, _Data) ->
    ok.


handle_event({call, From}, {recv, {Tag, _} = TM}, _, _) ->
    {Decoded, <<>>} = demarshal(TM),
    {keep_state_and_data, [{reply, From, ok}, nei({recv, {Tag, Decoded}})]};

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

handle_event(internal, {response, #{label := pgmp_socket, reply := ok}}, _, _) ->
    keep_state_and_data;

handle_event(internal,
             {send, Message},
             _,
             #{requests := Requests, socket := Socket} = Data) ->
    {keep_state,
     Data#{requests := pgmp_socket:send(
                         #{server_ref => Socket,
                           data => Message,
                           requests => Requests})}};
handle_event(internal,
             {recv, {parameter_status, {K, V}}},
             _,
             #{parameters := Parameters} = Data) ->
    {keep_state, Data#{parameters := Parameters#{K => V}}};

handle_event(internal, terminate, _, _) ->
    {keep_state_and_data,
     nei({send, [<<$X>>, prefix_with_size([])]})};

handle_event(internal, {recv, {notice_response, _} = TM}, query, _) ->
    {keep_state_and_data, nei({process, TM})};

handle_event(internal, {process, Reply}, _, #{from := _, replies := Rs} = Data) ->
    {keep_state, Data#{replies := [Reply | Rs]}};

handle_event(internal,
             {response, #{label := pgmp_types, reply := ready}},
             _,
             #{types_ready := Missing} = Data) when Missing == false ->
    {keep_state, Data#{types_ready => not(Missing)}};

handle_event(internal, types_when_ready, _, Data) ->
    {keep_state, Data#{requests := pgmp_types:when_ready(
                                     maps:with([requests], Data))}};

handle_event(enter, _, {ready_for_query, _}, Data) ->
    {keep_state,
     maps:with([ancestors,
                backend,
                cache,
                config,
                parameters,
                requests,
                socket,
                supervisor,
                types_ready],
               Data)};

handle_event(enter, _, _, _) ->
    keep_state_and_data.
