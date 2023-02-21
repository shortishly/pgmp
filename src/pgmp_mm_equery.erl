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


-module(pgmp_mm_equery).


-export([callback_mode/0]).
-export([handle_event/4]).
-export([terminate/3]).
-import(pgmp_codec, [marshal/2]).
-import(pgmp_codec, [size_inclusive/1]).
-import(pgmp_data_row, [decode/2]).
-import(pgmp_mm_common, [actions/3]).
-import(pgmp_mm_common, [data/3]).
-import(pgmp_mm_common, [field_names/1]).
-import(pgmp_statem, [nei/1]).
-include_lib("kernel/include/logger.hrl").


%% https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY


callback_mode() ->
    [handle_event_function, state_enter].


terminate(Reason, State, Data) ->
    pgmp_mm_common:terminate(Reason, State, Data).

handle_event({call, _} = Call,
             {request, #{action := query = Action} = Arg},
             unsynchronized,
             Data) ->
    {next_state,
     Action,
     data(Call, Arg, Data),
     [pop_callback_module | actions(Call, Arg, Data)]};

handle_event({call, From},
             {request, #{action := sync}},
             unsynchronized,
             _) ->
    {keep_state_and_data, [{reply, From, ok}, nei(sync)]};

handle_event({call, _} = Call,
             {request, #{action := Action} = Arg},
             unsynchronized,
             Data)
  when Action == parse;
       Action == describe;
       Action == bind;
       Action == execute ->
    {next_state,
     Action,
     data(Call, Arg, Data),
     actions(Call, Arg, Data)};

handle_event({call, From},
             {request, #{action := parameters}},
             _,
             #{parameters := Parameters}) ->
    {keep_state_and_data, {reply, From, Parameters}};

handle_event({call, _}, {request, _}, _, _) ->
    {keep_state_and_data, postpone};

handle_event(internal, flush, _, _) ->
    {keep_state_and_data,
     nei({send, ["H", size_inclusive([])]})};

handle_event(internal, sync, _, _) ->
    {keep_state_and_data,
     [nei({send, ["S", size_inclusive([])]}), nei(gc_unnamed_portal)]};

handle_event(internal, gc_unnamed_portal, _, #{cache := Cache}) ->
    ets:delete(Cache, {parameter_description, <<>>}),
    ets:delete(Cache, {row_description, <<>>}),
    keep_state_and_data;

handle_event(internal, {describe, [What, Name]}, _, _) ->
    {keep_state_and_data,
     nei({send, ["D", size_inclusive([What, marshal(string, Name)])]})};

handle_event(internal, {describe_statement, Statement}, unsynchronized, Data) ->
    Args = [$S, Statement],
    {next_state,
     describe_statement,
     Data#{args => Args},
     [nei({describe, Args}), nei(flush)]};

handle_event(internal, {describe_portal, Portal}, unsynchronized, Data) ->
    Args = [$P, Portal],
    {next_state,
     describe_portal,
     Data#{args => Args},
     [nei({describe, Args}), nei(flush)]};

handle_event(internal, {parse = EventName, [Name, SQL]}, _, _) ->
    {keep_state_and_data,
     [nei({telemetry,
           EventName,
           #{count => 1},
           #{args => #{name => Name, sql => SQL}}}),
      nei({send,
           ["P",
            size_inclusive(
              [marshal(string, Name),
               marshal(string, SQL),
               marshal(int16, 0)])]})]};

handle_event(internal,
             {bind,
              [Statement,
               Portal,
               Values,
               ParameterFormat,
               ResultFormat]},
             _,
             #{cache := Cache, parameters := Parameters}) ->
    case ets:lookup(Cache, {parameter_description, Statement}) of

        [{_, Types}]
          when length(Types) == length(Values),
               length(Types) == 0 ->
            {keep_state_and_data,
             nei({send,
                  ["B",
                   size_inclusive(
                     [marshal(string, Portal),
                      marshal(string, Statement),
                      marshal(int16, 0),
                      marshal(int16, length(Values)),
                      marshal(int16, 1),
                      marshal(int16, 1)])]})};

        [{_, Types}]
          when length(Types) == length(Values),
               length(Types) > 0 ->
            {keep_state_and_data,
             nei({send,
                  ["B",
                   size_inclusive(
                     [marshal(string, Portal),
                      marshal(string, Statement),
                      marshal(int16, 1),
                      marshal(int16, format(ParameterFormat)),

                      marshal(int16, length(Values)),

                      lists:foldl(
                        fun
                            ({_, null}, A) ->
                                [A, marshal(int32, -1)];

                            ({TypeOID, Value}, A) ->
                                Encoded = pgmp_data_row:encode(
                                            Parameters,
                                            [{#{format => ParameterFormat,
                                                type_oid => TypeOID},
                                              Value}]),
                                [A,
                                 marshal(int32, iolist_size(Encoded)),
                                 Encoded]
                        end,
                        [],
                        lists:zip(Types, Values)),
                      marshal(int16, 1),
                      marshal(
                        int16,
                        format(ResultFormat))])]})}
    end;

handle_event(internal, {execute = EventName, [Portal, MaxRows]}, _, _) ->
    {keep_state_and_data,
     [nei({telemetry,
           EventName,
           #{count => 1},
           #{args => #{portal => Portal, max_rows => MaxRows}}}),

      nei({send,
           ["E",
            size_inclusive(
              [marshal(string, Portal),
               marshal(int32, MaxRows)])]})]};

handle_event(internal,
             {recv = EventName, {ready_for_query = Tag, _} = TM},
             State,
             Data)
  when State == unsynchronized;
       State == error ->
    {next_state,
     TM,
     Data,
     [pop_callback_module,
      nei({telemetry,
           EventName,
           #{count => 1},
           #{tag => Tag}})]};

handle_event(internal,
             {recv = EventName, {data_row = Tag, Columns}},
             execute,
             #{parameters := Parameters, types := Types}) ->
    {keep_state_and_data,
     [nei({telemetry, EventName, #{count => 1}, #{tag => Tag}}),
      nei({process, {Tag, decode(Parameters, lists:zip(Types, Columns))}})]};

handle_event(internal,
             {recv = EventName, {parse_complete = Tag, _}},
             {named_statements, _},
             #{args := [Statement, _]} = Data) ->
    Args = [$S, Statement],
    {keep_state,
     Data#{args := Args},
     [nei({telemetry, EventName, #{count => 1}, #{tag => Tag}}),
      nei({describe, Args}),
      nei(flush)]};

handle_event(internal,
             {recv = EventName, {parameter_description = Tag, Decoded}},
             {named_statements, _},
             #{args := [$S, Statement],
               cache := Cache}) ->
    ets:insert(Cache, {{Tag, Statement}, Decoded}),
    {keep_state_and_data,
     nei({telemetry, EventName, #{count => 1}, #{tag => Tag}})};

handle_event(internal,
             {recv = EventName, {row_description = Tag, Decoded}},
             {named_statements, _},
             #{args := [$S, Statement], cache := Cache} = Data) ->
    ets:insert(Cache, {{Tag, Statement}, Decoded}),
    {keep_state,
     maps:without([args], Data),
     [nei({telemetry, EventName, #{count => 1}, #{tag => Tag}}),
      nei(next_named)]};

handle_event(internal,
             {recv = EventName, {no_data = Tag, _}},
             {named_statements, _},
             #{args := [$S, _]} = Data) ->
    {keep_state,
     maps:without([args], Data),
     [nei({telemetry, EventName, #{count => 1}, #{tag => Tag}}),
      nei(next_named)]};

handle_event(internal,
             {recv = EventName, {ready_for_query = Tag, idle}},
             {named_statements, Previous},
             Data) ->
    {next_state,
     Previous,
     Data,
     [pop_callback_module,
      nei({telemetry, EventName, #{count => 1}, #{tag => Tag}})]};

handle_event(internal,
             next_named,
             {named_statements, _},
             #{named := Named} = Data) ->
    case maps:next(Named) of
        {Statement, SQL, Iterator} ->
            {keep_state,
             Data#{named := Iterator,
                   args =>  [Statement, SQL]},
             [nei({parse, [Statement, SQL]}), nei(flush)]};

        none ->
            {keep_state, maps:without([named], Data), nei(sync)}
    end;

handle_event(internal,
             {recv = EventName, {parse_complete = Tag, _} = TM},
             parse = Action,
             #{args := [Statement, _]} = Data) ->
    {next_state,
     unsynchronized,
     Data,
     [nei({telemetry, EventName, #{count => 1}, #{tag => Tag}}),
      nei({process, TM}),
      nei({complete, Action}),
      nei({describe_statement, Statement}),
      nei({sync_when_named, Statement})]};

handle_event(internal, {sync_when_named, <<>>}, _, _) ->
    keep_state_and_data;

handle_event(internal, {sync_when_named, _}, _, _) ->
    {keep_state_and_data, nei(sync)};

handle_event(internal,
             {recv = EventName, {error_response = Tag, _} = TM},
             parse = Action,
             Data) ->
    {next_state,
     error,
     Data,
     [nei({telemetry, EventName, #{count => 1}, #{tag => Tag}}),
      nei({process, TM}),
      nei({complete, Action}),
      nei(sync)]};

handle_event(internal,
             {recv = EventName, {bind_complete = Tag, _} = Reply},
             bind = Action,
             #{args := [_, Portal, _, _, _]} = Data) ->
    {next_state,
     unsynchronized,
     Data,
     [nei({telemetry, EventName, #{count => 1}, #{tag => Tag}}),
      nei({process, Reply}),
      nei({complete, Action}),
      nei({describe_portal, Portal}),
      nei({sync_when_named, Portal})]};

handle_event(internal,
             {recv = EventName, {error_response = Tag, _} = Reply},
             bind = Action,
             #{args := [_, Portal, _, _, _], cache := Cache} = Data) ->
    ets:delete(Cache, {parameter_description, Portal}),
    ets:delete(Cache, {row_description, Portal}),
    {next_state,
     error,
     Data,
     [nei({telemetry, EventName, #{count => 1}, #{tag => Tag}}),
      nei({process, Reply}),
      nei({complete, Action}),
      nei(sync)]};

handle_event(internal,
             {recv = EventName, {error_response = Tag, _} = Reply},
             execute = Action,
             #{args := [Portal, _], cache := Cache} = Data) ->
    ets:delete(Cache, {parameter_description, Portal}),
    ets:delete(Cache, {row_description, Portal}),
    {next_state,
     error,
     Data,
     [nei({telemetry, EventName, #{count => 1}, #{tag => Tag}}),
      nei({process, Reply}),
      nei({complete, Action}),
      nei(sync)]};

handle_event(internal,
             {recv = EventName, {data_row = Tag, Columns}},
             execute,
             #{args := [Portal, _],
               parameters := Parameters,
               cache := Cache} = Data) ->
    [{_, Types}] = ets:lookup(Cache, {row_description, Portal}),
    {keep_state,
     Data#{types => Types},
     [nei({telemetry, EventName, #{count => 1}, #{tag => Tag}}),
      nei({process, {row_description, field_names(Types)}}),
      nei({process, {Tag, decode(Parameters, lists:zip(Types, Columns))}})]};

handle_event(internal,
             {recv = EventName,
              {command_complete = Tag, {Command, Rows}} = Reply},
             execute = Action,
             #{span := #{metadata := Metadata,
                         measurements := Measurements} = Span} = Data)
  when is_atom(Command),
       is_integer(Rows) ->
    {next_state,
     unsynchronized,
     Data#{span := Span#{metadata := Metadata#{command => Command},
                         measurements := Measurements#{rows => Rows}}},
     [nei({telemetry,
           EventName,
           #{count => 1, rows => Rows},
           #{tag => Tag, command => Command}}),
      nei({process, Reply}),
      nei({complete, Action})]};

handle_event(internal,
             {recv = EventName, {Tag, _} = Reply},
             execute = Action,
             Data)
  when Tag == portal_suspended;
       Tag == empty_query_response;
       Tag == command_complete ->
    {next_state,
     unsynchronized,
     Data,
     [nei({telemetry, EventName, #{count => 1}, #{tag => Tag}}),
      nei({process, Reply}),
      nei({complete, Action})]};

handle_event(internal, {recv = EventName, {Tag, _} = Reply}, describe, _)
  when Tag == parameter_description ->
    {keep_state_and_data,
     [nei({telemetry, EventName, #{count => 1}, #{tag => Tag}}),
      nei({process, Reply})]};

handle_event(internal,
             {recv = EventName, {error_response = Tag, _} = Reply},
             describe = Action,
             Data) ->
    {next_state,
     error,
     Data,
     [nei({telemetry, EventName, #{count => 1}, #{tag => Tag}}),
      nei({process, Reply}),
      nei({complete, Action}),
      nei(sync)]};

handle_event(internal,
             {recv = EventName, {Tag, _} = Reply},
             describe = Action,
             Data)
  when Tag == row_description;
       Tag == no_data  ->
    {next_state,
     unsynchronized,
     Data,
     [nei({telemetry, EventName, #{count => 1}, #{tag => Tag}}),
      nei({process, Reply}),
      nei({complete, Action})]};

handle_event(internal,
             {recv = EventName, {parameter_description = Tag, Decoded}},
             describe_statement,
             #{args := [$S, Statement],
               cache := Cache}) ->
    ets:insert(Cache, {{Tag, Statement}, Decoded}),
    {keep_state_and_data,
     nei({telemetry, EventName, #{count => 1}, #{tag => Tag}})};

handle_event(internal,
             {recv = EventName, {row_description = Tag, Decoded}},
             describe_statement,
             #{args := [$S, Statement],
               cache := Cache} = Data) ->
    ets:insert(Cache, {{Tag, Statement}, Decoded}),
    {next_state,
     unsynchronized,
     Data,
     nei({telemetry, EventName, #{count => 1}, #{tag => Tag}})};

handle_event(internal,
             {recv = EventName, {no_data = Tag, _}},
             describe_statement,
             #{args := [$S, _]} = Data) ->
    {next_state,
     unsynchronized,
     Data,
     nei({telemetry, EventName, #{count => 1}, #{tag => Tag}})};

handle_event(internal,
             {recv = EventName, {parameter_description = Tag, Decoded}},
             describe_portal,
             #{args := [$P, Portal],
               cache := Cache}) ->
    ets:insert(Cache, {{Tag, Portal}, Decoded}),
    {keep_state_and_data,
     nei({telemetry, EventName, #{count => 1}, #{tag => Tag}})};

handle_event(internal,
             {recv = EventName, {row_description = Tag, Decoded}},
             describe_portal,
             #{args := [$P, Portal],
               cache := Cache} = Data) ->
    ets:insert(Cache, {{Tag, Portal}, Decoded}),
    {next_state,
     unsynchronized,
     Data,
     nei({telemetry, EventName, #{count => 1}, #{tag => Tag}})};

handle_event(internal,
             {recv = EventName, {no_data = Tag, _}},
             describe_portal,
             #{args := [$P, _]} = Data) ->
    {next_state,
     unsynchronized,
     Data,
     nei({telemetry, EventName, #{count => 1}, #{tag => Tag}})};

handle_event(internal,
             {recv = EventName, {error_response = Tag, _}},
             describe_portal,
             #{args := [$P, _]} = Data) ->
    {next_state,
     error,
     Data,
     nei({telemetry, EventName, #{count => 1}, #{tag => Tag}})};

handle_event(internal,
             {complete, Action},
             _,
             #{replies := Replies, from := From} = Data) ->
    {keep_state,
     maps:without([from, replies], Data),
     [{reply, From, lists:reverse(Replies)}, nei({span_stop, Action})]};

handle_event(enter, _, unsynchronized, Data) ->
    {keep_state, maps:without([args, types], Data)};

handle_event(Type, Content, State, Data) ->
    pgmp_mm_common:handle_event(Type, Content, State, Data).


format(text) ->
    0;

format(binary) ->
    1.
