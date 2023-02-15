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
-import(pgmp_codec, [size_inclusive/1]).
-import(pgmp_statem, [nei/1]).
-include_lib("kernel/include/logger.hrl").


field_names(Types) ->
    [Name || #{field_name := Name} <- Types].


data({call, From}, #{args := Args}, Data) ->
    Data#{from => From, args => Args, replies => []}.


actions({call, _} = Call, Arg, Data) ->
    [span_start(Call, Arg, Data),
     command(Call, Arg, Data) | post_actions(Call, Arg, Data)].


command(_, #{action := Action, args := Args}, _) ->
    nei({Action, Args}).


span_start(_, #{action := Action}, _) ->
    nei({?FUNCTION_NAME, Action}).


post_actions(_, #{action := Action}, _) ->
    [nei(flush) || Action == parse
                       orelse Action == describe
                       orelse Action == bind
                       orelse Action == execute].


terminate(_Reason, _State, #{config := #{group := Group}}) ->
    pgmp_pg:leave(Group);

terminate(_Reason, _State, _Data) ->
    ok.


handle_event(internal,
             {response, #{label := pgmp_types, reply := ready}},
             _,
             #{types_ready := false} = Data) ->
    {keep_state, Data#{types_ready := true}};

handle_event({call, From}, {recv, {Tag, _} = TM}, _, _) ->
    {Decoded, <<>>} = demarshal(TM),
    {keep_state_and_data, [{reply, From, ok}, nei({recv, {Tag, Decoded}})]};

handle_event(info, Msg, _, #{requests := Existing} = Data) ->
    case gen_statem:check_response(Msg, Existing, true) of
        {{reply, Reply}, Label, Updated} ->
            {keep_state,
             Data#{requests := Updated},
             nei({response, #{label => Label, reply => Reply}})};

        {{error, {Reason, _}}, _, UpdatedRequests} ->
            {stop, Reason, Data#{requests := UpdatedRequests}}
    end;

handle_event(internal,
             {response, #{label := pgmp_socket, reply := ok}},
             _,
             _) ->
    keep_state_and_data;

handle_event(internal,
             {response, #{label := pgmp_connection,
                          reply := ok}},
             _,
             _) ->
    keep_state_and_data;

handle_event(internal,
             {send = EventName, Message},
             _,
             #{requests := Requests, socket := Socket} = Data) ->
    {keep_state,
     Data#{requests := pgmp_socket:send(
                         #{server_ref => Socket,
                           data => Message,
                           requests => Requests})},
     nei({telemetry, EventName, #{message => Message}})};

handle_event(internal,
             {recv, {parameter_status, {K, V}}},
             _,
             #{parameters := Parameters} = Data) ->
    {keep_state, Data#{parameters := Parameters#{K => V}}};

handle_event(internal, terminate, _, _) ->
    {keep_state_and_data,
     nei({send, ["X", size_inclusive([])]})};

handle_event(internal,
             {recv, {notice_response, _} = TM},
             State,
             _) when State == query; State == execute ->
    {keep_state_and_data, nei({process, TM})};

handle_event(internal,
             {process, {Tag, _} = Reply},
             _,
             #{from := _, replies := Rs} = Data) when Tag == error_response;
                                                      Tag == notice_response ->
    {keep_state,
     Data#{replies := [pgmp_error_notice_fields:map(Reply) | Rs]}};

handle_event(internal, {process, Reply}, _, #{from := _, replies := Rs} = Data) ->
    {keep_state, Data#{replies := [Reply | Rs]}};

handle_event(internal, types_when_ready, _, Data) ->
    {keep_state, Data#{requests := pgmp_types:when_ready(
                                     maps:with([requests], Data))}};

handle_event(enter,
             _,
             {ready_for_query, State},
             #{requests := Requests,
               config := #{group := _}} = Data) ->
    {keep_state,
     maps:with(
       [ancestors,
        backend,
        cache,
        config,
        parameters,
        requests,
        socket,
        supervisor,
        types_ready],
       Data#{requests => pgmp_connection:ready_for_query(
                           #{state => State,
                             requests => Requests})})};

handle_event(enter,
             _,
             {ready_for_query, _},
             Data) ->
    {keep_state,
     maps:with(
       [ancestors,
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
    keep_state_and_data;

handle_event(internal,
             {span_start, Action},
              _,
             Data) ->
    StartTime = erlang:monotonic_time(),
    Metadata = maps:with([args], Data),
    {keep_state,
     Data#{span => #{start_time => StartTime,
                     measurements => #{},
                     metadata => Metadata}},
     nei({telemetry,
          [Action, start],
          #{monotonic_time => StartTime},
          Metadata})};

handle_event(internal,
             {span_stop, Action},
             _,
             #{span := #{start_time := StartTime,
                         measurements := Measurements,
                         metadata := Metadata}} = Data) ->
    StopTime = erlang:monotonic_time(),
    {keep_state,
     maps:without([span], Data),
     nei({telemetry,
          [Action, stop],
          maps:merge(
            #{duration => StopTime - StartTime,
              monotonic_time => StopTime},
            Measurements),
          Metadata})};

handle_event(internal, {span_stop, _}, _, _) ->
    keep_state_and_data;

handle_event(internal,
             {telemetry, EventName, Measurements},
             _,
             _) ->
    {keep_state_and_data,
     nei({telemetry, EventName, Measurements, #{}})};

handle_event(internal,
             {telemetry, EventName, Measurements, Metadata},
             _,
             _) when is_atom(EventName) ->
    {keep_state_and_data,
     nei({telemetry, [EventName], Measurements, Metadata})};

handle_event(internal,
             {telemetry, EventName, Measurements, Metadata},
             _,
             Data) ->
    ok = telemetry:execute(
           [pgmp, mm] ++ EventName,
           Measurements,
           maps:merge(
             maps:with([socket], Data),
             args(EventName, Metadata))),
    keep_state_and_data;

handle_event(internal, {recv, {error_response, _} = TM}, _, Data) ->
    {Tag, Message} = pgmp_error_notice_fields:map(TM),
    ?LOG_WARNING(#{tag => Tag, message  => Message}),
    {next_state,
     limbo,
     Data,
     [nei({telemetry,
           error,
           #{count => 1},
           maps:merge(
             #{event => Tag},
             maps:with(
               [code, message, severity],
               Message))}),
      {state_timeout,
       timer:seconds(
         backoff:rand_increment(
           pgmp_config:backoff(rand_increment))),
       {backoff, #{action => Tag, reason => Message}}}]};

handle_event(state_timeout, {backoff, _}, limbo, _) ->
    stop.


args([bind | _],
     #{args := [Statement,
                Portal,
                Values,
                ParameterFormat,
                ResultFormat]} = Metadata) ->
    Metadata#{args := #{statement => Statement,
                        portal => Portal,
                        values => Values,
                        format => #{parameter => ParameterFormat,
                                    result => ResultFormat}}};

args([execute | _], #{args := [Portal, MaxRows]} = Metadata) ->
    Metadata#{args := #{portal => Portal, max_rows => MaxRows}};

args([parse | _], #{args := [Name, SQL]} = Metadata) ->
    Metadata#{args := #{name => Name, sql => SQL}};

args([query | _], #{args := [SQL]} = Metadata) ->
    Metadata#{args := #{sql => SQL}};

args(_, Metadata) ->
    Metadata.
