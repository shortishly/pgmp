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


-module(pgmp_mm_extended_query).


-export([callback_mode/0]).
-export([handle_event/4]).
-export([terminate/3]).
-import(pgmp_codec, [demarshal/1]).
-import(pgmp_codec, [marshal/2]).
-import(pgmp_codec, [size_inclusive/1]).
-import(pgmp_data_row, [decode/2]).
-import(pgmp_mm_common, [actions/3]).
-import(pgmp_mm_common, [data/3]).
-import(pgmp_mm_common, [field_names/1]).
-import(pgmp_statem, [nei/1]).
-include_lib("kernel/include/logger.hrl").


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

handle_event({call, _}, {request, _}, _, _) ->
    {keep_state_and_data, postpone};

handle_event(internal, flush, _, _) ->
    {keep_state_and_data,
     nei({send, [<<$H>>, size_inclusive([])]})};

handle_event(internal, sync, _, _) ->
    {keep_state_and_data,
     [nei({send, [<<$S>>, size_inclusive([])]}), nei(gc_unnamed_portal)]};

handle_event(internal, gc_unnamed_portal, _, #{cache := Cache}) ->
    ets:delete(Cache, {parameter_description, <<>>}),
    ets:delete(Cache, {row_description, <<>>}),
    keep_state_and_data;

handle_event(internal, {describe, [What, Name]}, _, _) ->
    {keep_state_and_data,
     nei({send, [<<$D>>, size_inclusive([What, marshal(string, Name)])]})};

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

handle_event(internal, {parse, [Name, SQL]}, _, _) ->
    {keep_state_and_data,
     nei({send,
          [<<$P>>,
           size_inclusive(
             [marshal(string, Name),
              marshal(string, SQL),
              marshal({int, 16}, 0)])]})};

handle_event(internal,
             {bind, [Portal, Statement, Values]},
             _,
             #{cache := Cache, parameters := Parameters}) ->
    case ets:lookup(Cache, {parameter_description, Statement}) of

        [{_, Types}]
          when length(Types) == length(Values),
               length(Types) == 0 ->
            {keep_state_and_data,
             nei({send,
                  [<<$B>>,
                   size_inclusive(
                     [marshal(string, Portal),
                      marshal(string, Statement),
                      marshal({int, 16}, 0),
                      marshal({int, 16}, length(Values)),
                      marshal({int, 16}, 1),
                      marshal({int, 16}, 1)])]})};

        [{_, Types}]
          when length(Types) == length(Values),
               length(Types) > 0 ->
            {keep_state_and_data,
             nei({send,
                  [<<$B>>,
                   size_inclusive(
                     [marshal(string, Portal),
                      marshal(string, Statement),
                      marshal(int16, 1),
                      marshal(int16, 1),

                      marshal(int16, length(Values)),
                      lists:foldl(
                        fun
                            ({_, null}, A) ->
                                [A, marshal(int32, -1)];

                            ({TypeOID, Value}, A) ->
                                Encoded = pgmp_data_row:encode(
                                            Parameters,
                                            [{#{format => binary,
                                                type_oid => TypeOID},
                                              Value}]),
                                [A,
                                 marshal(int32, iolist_size(Encoded)),
                                 Encoded]
                        end,
                        [],
                        lists:zip(Types, Values)),
                      marshal(int16, 1),
                      marshal(int16, 1)])]})}
    end;

handle_event(internal, {execute, [Portal, MaxRows]}, _, _) ->
    {keep_state_and_data,
     nei({send,
          [<<$E>>,
           size_inclusive(
             [marshal(string, Portal),
              marshal({int, 32}, MaxRows)])]})};

handle_event(internal, {recv, {ready_for_query, _} = TM}, unsynchronized, Data) ->
    {next_state, TM, Data, pop_callback_module};

handle_event(internal,
             {recv, {data_row = Tag, Columns}},
             execute,
             #{parameters := Parameters, types := Types}) ->
    {keep_state_and_data,
     nei({process, {Tag, decode(Parameters, lists:zip(Types, Columns))}})};

handle_event(internal,
             {recv, {parse_complete, _} = TM},
             parse,
             #{args := [Statement, _]} = Data) ->
    {next_state,
     unsynchronized,
     Data,
     [nei({process, TM}),
      nei(complete),
      nei({describe_statement, Statement}),
      nei({sync_when_named, Statement})]};

handle_event(internal, {sync_when_named, <<>>}, _, _) ->
    keep_state_and_data;

handle_event(internal, {sync_when_named, _}, _, _) ->
    {keep_state_and_data, nei(sync)};

handle_event(internal, {recv, {error_response, _} = TM}, parse, Data) ->
    {next_state,
     unsynchronized,
     Data,
     [nei({process, TM}), nei(complete), nei(sync)]};

handle_event(internal,
             {recv, {bind_complete, _} = Reply},
             bind,
             #{args := [Portal, _, _]} = Data) ->
    {next_state,
     unsynchronized,
     Data,
     [nei({process, Reply}),
      nei(complete),
      nei({describe_portal, Portal}),
      nei({sync_when_named, Portal})]};

handle_event(internal,
             {recv, {error_response, _} = Reply},
             bind,
             #{args := [Portal, _, _], cache := Cache} = Data) ->
    ets:delete(Cache, {parameter_description, Portal}),
    ets:delete(Cache, {row_description, Portal}),
    {next_state,
     unsynchronized,
     Data,
     [nei({process, Reply}), nei(complete), nei(sync)]};

handle_event(internal,
             {recv, {error_response, _} = Reply},
             execute,
             #{args := [Portal, _], cache := Cache}) ->
    ets:delete(Cache, {parameter_description, Portal}),
    ets:delete(Cache, {row_description, Portal}),
    {keep_state_and_data, [nei({process, Reply}), nei(complete), nei(sync)]};

handle_event(internal,
             {recv, {data_row = Tag, Columns}},
             execute,
             #{args := [Portal, _],
               parameters := Parameters,
               cache := Cache} = Data) ->
    [{_, Types}] = ets:lookup(Cache, {row_description, Portal}),
    {keep_state,
     Data#{types => Types},
     [nei({process, {row_description, field_names(Types)}}),
      nei({process, {Tag, decode(Parameters, lists:zip(Types, Columns))}})]};

handle_event(internal, {recv, {Tag, _} = Reply}, execute, Data)
  when Tag == portal_suspended;
       Tag == empty_query_response;
       Tag == command_complete ->
    {next_state,
     unsynchronized,
     Data,
     [nei({process, Reply}), nei(complete)]};

handle_event(internal, {recv, {Tag, _} = Reply}, describe, _)
  when Tag == parameter_description ->
    {keep_state_and_data, nei({process, Reply})};

handle_event(internal, {recv, {error_response, _} = Reply}, describe, Data) ->
    {next_state,
     unsynchronized,
     Data,
     [nei({process, Reply}), nei(complete), nei(sync)]};

handle_event(internal, {recv, {Tag, _} = Reply}, describe, Data)
  when Tag == row_description;
       Tag == no_data  ->
    {next_state,
     unsynchronized,
     Data,
     [nei({process, Reply}), nei(complete)]};

handle_event(internal,
             {recv, {parameter_description = Tag, Decoded}},
             describe_statement,
             #{args := [$S, Statement],
               cache := Cache}) ->
    ets:insert(Cache, {{Tag, Statement}, Decoded}),
    keep_state_and_data;

handle_event(internal,
             {recv, {row_description = Tag, Decoded}},
             describe_statement,
             #{args := [$S, Statement],
               cache := Cache} = Data) ->
    ets:insert(Cache, {{Tag, Statement}, Decoded}),
    {next_state, unsynchronized, Data};

handle_event(internal,
             {recv, {no_data, _}},
             describe_statement,
             #{args := [$S, _]} = Data) ->
    {next_state, unsynchronized, Data};

handle_event(internal,
             {recv, {parameter_description = Tag, Decoded}},
             describe_portal,
             #{args := [$P, Portal],
               cache := Cache}) ->
    ets:insert(Cache, {{Tag, Portal}, Decoded}),
    keep_state_and_data;

handle_event(internal,
             {recv, {row_description = Tag, Decoded}},
             describe_portal,
             #{args := [$P, Portal],
               cache := Cache} = Data) ->
    ets:insert(Cache, {{Tag, Portal}, Decoded}),
    {next_state, unsynchronized, Data};

handle_event(internal,
             {recv, {no_data, _}},
             describe_portal,
             #{args := [$P, _]} = Data) ->
    {next_state, unsynchronized, Data};

handle_event(internal,
             {recv, {error_response, _}},
             describe_portal,
             #{args := [$P, _]} = Data) ->
    {next_state, unsynchronized, Data};

handle_event(internal, complete, _, #{replies := Replies, from := From} = Data) ->
    {keep_state,
     maps:without([from, replies], Data),
     {reply, From, lists:reverse(Replies)}};

handle_event(enter, _, unsynchronized, Data) ->
    {keep_state, maps:without([args, types], Data)};

handle_event(Type, Content, State, Data) ->
    pgmp_mm_common:handle_event(Type, Content, State, Data).
