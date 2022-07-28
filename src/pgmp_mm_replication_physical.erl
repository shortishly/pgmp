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


-module(pgmp_mm_replication_physical).


-export([callback_mode/0]).
-export([handle_event/4]).
-export([terminate/3]).
-import(pgmp_codec, [demarshal/1]).
-import(pgmp_codec, [marshal/2]).
-import(pgmp_codec, [size_inclusive/1]).
-import(pgmp_data_row, [decode/2]).
-import(pgmp_mm_common, [field_names/1]).
-import(pgmp_statem, [nei/1]).
-include_lib("kernel/include/logger.hrl").


callback_mode() ->
    [handle_event_function, state_enter].


terminate(Reason, State, Data) ->
    pgmp_mm_common:terminate(Reason, State, Data).


handle_event(internal, {recv, {copy_both_response, _}}, _, _) ->
    keep_state_and_data;

handle_event(internal, {recv, {copy_data, _}}, _, _) ->
    keep_state_and_data;

handle_event(internal,
             {response, #{label := pgmp_types, reply := ready}},
             waiting_for_types,
             #{types_ready := Missing} = Data) when Missing == false ->
    {next_state,
     identify_system,
     Data#{types_ready => not(Missing)},
     nei(identify_system)};

handle_event(internal,
             bootstrap_complete,
             _,
             #{types_ready := false} = Data) ->
    {next_state, waiting_for_types, Data};

handle_event(internal,
             bootstrap_complete,
             _,
             #{types_ready := true} = Data) ->
    {next_state,
     identify_system,
     Data,
     nei(identify_system)};

handle_event(internal, identify_system, _, _) ->
    {keep_state_and_data, nei({query, [<<"IDENTIFY_SYSTEM">>]})};

handle_event(internal,
             {recv, {command_complete, Command}},
             _,
             Data) when Command == identify_system;
                        Command == create_replication_slot ->
    {keep_state, maps:without([columns], Data)};

handle_event(internal,
             {recv, {ready_for_query, idle}},
             identify_system,
             Data) ->
    {next_state, replication_slot, Data, nei(create_replication_slot)};

handle_event(internal,
             {recv, {ready_for_query, idle}},
             replication_slot,
             Data) ->
    {next_state, replication, Data, nei(start_replication)};

handle_event(internal, create_replication_slot = Command, _, _) ->
    {keep_state_and_data, nei({Command, <<"abc">>})};

handle_event(internal, {create_replication_slot, SlotName}, _, _) ->
    {keep_state_and_data,
     nei({query,
          [iolist_to_binary(
             io_lib:format(
               <<"CREATE_REPLICATION_SLOT ~s TEMPORARY PHYSICAL">>,
               [SlotName]))]})};

handle_event(internal,
             start_replication,
             _,
             #{identify_system := #{<<"xlogpos">> := Location},
               replication_slot := #{<<"slot_name">> := SlotName}}) ->
    {keep_state_and_data,
     nei({query,
          [iolist_to_binary(
             io_lib:format(
               <<"START_REPLICATION SLOT ~s PHYSICAL ~s">>,
               [SlotName, Location]))]})};

handle_event(internal,
             {recv, {ready_for_query, idle}},
             ignore_for_the_moment,
             #{identify_system := #{<<"timeline">> := Timeline}} = Data) ->
    {next_state,
     timeline_history,
     Data,
     nei({query,
          [iolist_to_binary(
             io_lib:format(
               <<"TIMELINE_HISTORY ~b">>,
               [Timeline]))]})};

handle_event(internal, {recv, {row_description, Columns}}, _, Data) ->
    {keep_state, Data#{columns => Columns}};

handle_event(internal,
             {recv, {data_row, Values}},
             State,
             #{parameters := Parameters,
               columns := Columns} = Data) ->
    {keep_state,
     maps:put(State,
              lists:foldl(
                fun
                    ({#{field_name := FieldName}, Value}, A) ->
                        A#{FieldName => Value}
                end,
                maps:get(State, Data, #{}),
                lists:zip(Columns,
                          decode(
                            Parameters,
                            lists:zip(Columns, Values)))),
              Data)};

handle_event(internal, {query, [SQL]}, _, _) ->
    {keep_state_and_data,
     nei({send, [<<$Q>>, size_inclusive([marshal(string, SQL)])]})};

handle_event(EventType, EventContent, State, Data) ->
    pgmp_mm_common:handle_event(EventType,
                                EventContent,
                                State,
                                Data).
