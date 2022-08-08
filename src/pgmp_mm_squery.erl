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


-module(pgmp_mm_squery).


-export([callback_mode/0]).
-export([handle_event/4]).
-export([terminate/3]).
-import(pgmp_codec, [marshal/2]).
-import(pgmp_codec, [size_inclusive/1]).
-import(pgmp_data_row, [decode/2]).
-import(pgmp_data_row, [decode/3]).
-import(pgmp_mm_common, [actions/3]).
-import(pgmp_mm_common, [data/3]).
-import(pgmp_mm_common, [field_names/1]).
-import(pgmp_statem, [nei/1]).
-include_lib("kernel/include/logger.hrl").


%% https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.4


callback_mode() ->
    [handle_event_function, state_enter].


terminate(Reason, State, Data) ->
    pgmp_mm_common:terminate(Reason, State, Data).


handle_event(internal, bootstrap_complete, _, _) ->
    keep_state_and_data;

handle_event(internal, {recv, {ready_for_query, _} = TM}, _, Data) ->
    {next_state, TM, Data};

handle_event({call, _} = Call,
             {request, #{action := query = Action} = Arg},
             {ready_for_query, _},
             Data) ->
    {next_state,
     Action,
     data(Call, Arg, Data),
     actions(Call, Arg, Data)};

handle_event({call, _},
             {request, #{action := Action}},
             {ready_for_query, _} = ReadyForQuery,
             #{types_ready := false} = Data)
  when Action == parse;
       Action == describe;
       Action == bind;
       Action == execute ->
    {next_state, {waiting_for_types, ReadyForQuery}, Data, postpone};

handle_event({call, _} = Call,
             {request, #{action := Action} = Arg},
             {ready_for_query, _},
             Data)
  when Action == parse;
       Action == describe;
       Action == bind;
       Action == execute ->
    {next_state,
     Action,
     data(Call, Arg, Data),
     [{push_callback_module, pgmp_mm_equery} | actions(Call, Arg, Data)]};

handle_event({call, _}, {request, _}, _, _) ->
    {keep_state_and_data, postpone};

handle_event(internal,
             {response, #{label := pgmp_types, reply := ready}},
             {waiting_for_types, ReadyForQuery},
             #{types_ready := false} = Data) ->
    {next_state, ReadyForQuery, Data#{types_ready := true}};

handle_event(internal,
             {response, #{label := pgmp_types, reply := ready}},
             _,
             #{types_ready := false} = Data) ->
    {keep_state, Data#{types_ready := true}};

handle_event(internal, {query, [SQL]}, _, _) ->
    {keep_state_and_data,
     nei({send, ["Q", size_inclusive([marshal(string, SQL)])]})};

handle_event(internal, {recv, {Tag, _} = TM}, query, _)
  when Tag == empty_query_response;
       Tag == error_response;
       Tag == command_complete ->
    {keep_state_and_data, [nei({process, TM}), nei(complete)]};

handle_event(internal, {recv, {row_description = Tag, Types}}, query, Data) ->
    {keep_state,
     Data#{types => Types},
     nei({process, {Tag, field_names(Types)}})};

handle_event(internal,
             {recv, {data_row = Tag, Columns}},
             query,
             #{parameters := Parameters,
               types_ready := true,
               types := Types}) ->
    {keep_state_and_data,
     nei({process,
          {Tag, decode(Parameters, lists:zip(Types, Columns))}})};

handle_event(internal,
             {recv, {data_row = Tag, Columns}},
             query,
             #{parameters := Parameters,
               types_ready := false,
               types := Types}) ->
    {keep_state_and_data,
     nei({process,
          {Tag, decode(Parameters, lists:zip(Types, Columns), #{})}})};

handle_event(internal, complete, _, #{replies := Replies, from := From} = Data) ->
    {keep_state,
     maps:without([args, from, replies, types], Data),
     {reply, From, lists:reverse(Replies)}};

handle_event(EventType, EventContent, State, Data) ->
    pgmp_mm_common:handle_event(EventType,
                                EventContent,
                                State,
                                Data).
