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


-export([callback_mode/0]).
-export([handle_event/4]).
-export([init/1]).
-export([request/2]).
-export([start_link/1]).
-export([terminate/3]).
-import(pgmp_codec, [demarshal/1]).
-import(pgmp_codec, [marshal/2]).
-import(pgmp_codec, [prefix_with_size/1]).
-import(pgmp_data_row, [decode/2]).
-import(pgmp_statem, [nei/1]).
-include_lib("kernel/include/logger.hrl").


start_link(Arg) ->
    gen_statem:start_link(?MODULE, [Arg], pgmp_config:options(?MODULE)).


request(MM, Arg) ->
    gen_statem:call(MM, {?FUNCTION_NAME, Arg}).


init([Arg]) ->
    process_flag(trap_exit, true),
    pg:join(pgmp_config:pg(scope), ?MODULE, self()),
    {ok,
     unready,
     Arg#{cache => ets:new(?MODULE, []),
          requests => gen_statem:reqids_new(),
          parameters => #{}},
     nei(peer)}.


callback_mode() ->
    [handle_event_function, state_enter].


terminate(_Reason, _State, _Data) ->
    pg:leave(pgmp_config:pg(scope), ?MODULE, self()).


handle_event(enter, _, {ready_for_query, _}, Data) ->
    {keep_state,
     maps:with([backend,
                cache,
                parameters,
                requests,
                socket,
                supervisor],
               Data)};

handle_event(enter, _, _, _) ->
    keep_state_and_data;

handle_event({call, From}, {recv, {Tag, _} = TM}, _, _) ->
    {Decoded, <<>>} = demarshal(TM),
    {keep_state_and_data, [{reply, From, ok}, nei({recv, {Tag, Decoded}})]};

handle_event({call, _} = Call,
             {request, #{action := Action} = Arg},
             {ready_for_query, _},
             Data) ->
    {next_state, Action, data(Call, Arg, Data), actions(Call, Arg, Data)};

handle_event({call, _} = Call,
             {request, #{action := Action} = Arg},
             unsynchronized,
             Data)
  when Action == parse;
       Action == describe;
       Action == bind;
       Action == execute ->
    {next_state, Action, data(Call, Arg, Data), actions(Call, Arg, Data)};

handle_event({call, _}, {request, _}, unsynchronized, _) ->
    {keep_state_and_data, [nei(sync), postpone]};

handle_event({call, _}, {request, _}, unready, Data) ->
    {next_state, starting, Data, [postpone, nei(startup)]};

handle_event({call, From},
             {request, _},
             startup_failure,
             #{errors := Errors}) ->
    {keep_state_and_data, {reply, From, [Errors]}};

handle_event({call, _}, {request, _}, _, _) ->
    {keep_state_and_data, postpone};

handle_event(internal, peer, _, #{supervisor := Supervisor} = Data) ->
    case pgmp_sup:get_child(Supervisor, socket) of
        {_, PID, worker, _} when is_pid(PID) ->
            {keep_state, Data#{socket => PID}};

        {_, _, _, _} = Reason ->
            {stop, Reason};

        false ->
            {stop, peer_not_found}
    end;

handle_event(info, Msg, _, #{requests := Existing} = Data) ->
    case gen_statem:check_response(Msg, Existing, true) of
        {{reply, ok}, _, Updated} ->
            {keep_state, Data#{requests := Updated}};

        {{error, {Reason, _}}, _From, UpdatedRequests} ->
                {stop, Reason, Data#{requests := UpdatedRequests}}
    end;

handle_event(internal,
             {send, _} = Request,
             _,
             #{requests := Requests, socket := Socket} = Data) ->
    {keep_state,
     Data#{requests := gen_statem:send_request(
                         Socket,
                         Request,
                         make_ref(),
                         Requests)}};

handle_event(internal, startup, _, _) ->
    {keep_state_and_data, nei({startup, #{}})};

handle_event(internal, {startup, KV}, _, _) ->
    {keep_state_and_data,
     nei({send,
          prefix_with_size(
            [marshal({int, 32}, version()),
             maps:fold(
               fun
                   (K, V, A) ->
                       [marshal(string, K), marshal(string, V) | A]
               end,
               [],
               maps:merge(
                 #{<<"user">> => pgmp_config:database(user),
                   <<"database">> => pgmp_config:database(name)},
                 KV)),
             marshal(byte, <<0>>)])})};

handle_event(internal, flush, _, _) ->
    {keep_state_and_data,
     nei({send, [<<$H>>, prefix_with_size([])]})};

handle_event(internal, sync, _, _) ->
    {keep_state_and_data,
     [nei({send, [<<$S>>, prefix_with_size([])]}), nei(gc_unnamed_portal)]};

handle_event(internal, gc_unnamed_portal, _, #{cache := Cache}) ->
    ets:delete(Cache, {parameter_description, <<>>}),
    ets:delete(Cache, {row_description, <<>>}),
    keep_state_and_data;

handle_event(internal, terminate, _, _) ->
    {keep_state_and_data,
     nei({send, [<<$X>>, prefix_with_size([])]})};

handle_event(internal, {parse, [Name, SQL]}, _, _) ->
    {keep_state_and_data,
     nei({send,
          [<<$P>>,
           prefix_with_size(
             [marshal(string, Name),
              marshal(string, SQL),
              marshal({int, 16}, 0)])]})};

handle_event(internal, {describe, [What, Name]}, _, _) ->
    {keep_state_and_data,
     nei({send, [<<$D>>, prefix_with_size([What, marshal(string, Name)])]})};

handle_event(internal, {describe_portal, Portal}, unsynchronized, Data) ->
    Args = [$P, Portal],
    {next_state,
     describe_portal,
     Data#{args => Args},
     [nei({describe, Args}), nei(flush)]};

handle_event(internal, {query, [SQL]}, _, _) ->
    {keep_state_and_data,
     nei({send, [<<$Q>>, prefix_with_size([marshal(string, SQL)])]})};

handle_event(internal, {bind, [Portal, Statement, Parameters]}, _, _) ->
    {keep_state_and_data,
     nei({send,
          [<<$B>>,
           prefix_with_size(
             [marshal(string, Portal),
              marshal(string, Statement),
              marshal({int, 16}, 0),
              marshal({int, 16}, length(Parameters)),
              marshal({int, 16}, 1),
              marshal({int, 16}, 1)])]})};

handle_event(internal, {execute, [Portal, MaxRows]}, _, _) ->
    {keep_state_and_data,
     nei({send,
          [<<$E>>,
           prefix_with_size(
             [marshal(string, Portal),
              marshal({int, 32}, MaxRows)])]})};

handle_event(internal,
             {recv, {ready_for_query, _} = TM},
             _,
             #{replies := Replies, from := From} = Data) ->
    {next_state,
     TM,
     maps:without([from, replies], Data),
     {reply, From, lists:reverse(Replies)}};

handle_event(internal,
             {recv, {ready_for_query, _} = TM},
             _,
             #{stream := _, monitor := Monitor} = Data) ->
    true = erlang:demonitor(Monitor),
    {next_state, TM, maps:without([stream, monitor], Data)};

handle_event(internal, {recv, {ready_for_query, _} = TM}, _, Data) ->
    {next_state, TM, Data};

handle_event(internal, {recv, {authentication, authenticated}}, _, Data) ->
    {next_state, authenticated, Data};

handle_event(internal,
             {recv, {authentication, {sasl, _} = SASL}},
             _,
             _) ->
    {keep_state_and_data,
     [{push_callback_module, pgmp_mm_auth_sasl}, nei(SASL)]};

handle_event(internal,
             {recv, {authentication, {md5_password, _} = MD5}},
             _,
             _) ->
    {keep_state_and_data,
     [{push_callback_module, pgmp_mm_auth_md5}, nei(MD5)]};

handle_event(internal,
             {recv, {parameter_status, {K, V}}},
             _,
             #{parameters := Parameters} = Data) ->
    {keep_state, Data#{parameters := Parameters#{K => V}}};

handle_event(internal,
             {recv, {backend_key_data, [PID, Key]}},
             _,
             Data) ->
    {keep_state, Data#{backend => #{pid => PID, key => Key}}};

handle_event(internal, {process, Reply}, _, #{from := _, replies := Rs} = Data) ->
    {keep_state, Data#{replies := [Reply | Rs]}};

handle_event(internal, {process, Reply}, _, #{stream := {Ref, PID}})
  when is_reference(Ref), is_pid(PID) ->
    try
        erlang:send(PID, {Ref, Reply}),
        keep_state_and_data

    catch
        Class:Exception:Stacktrace ->
            {stop, #{class => Class,
                     exception => Exception,
                     stack_trace => Stacktrace}}
    end;

handle_event(internal, {process, Reply}, _, #{stream := PID})
  when is_pid(PID) ->
    try
        erlang:send(PID, Reply),
        keep_state_and_data

    catch
        Class:Exception:Stacktrace ->
            {stop, #{class => Class,
                     exception => Exception,
                     stack_trace => Stacktrace}}
    end;

handle_event(internal, complete, _, #{replies := Replies, from := From} = Data) ->
    {keep_state,
     maps:without([from, replies], Data),
     {reply, From, lists:reverse(Replies)}};

handle_event(internal, complete, _, #{stream := _, monitor := Monitor} = Data) ->
    true = erlang:demonitor(Monitor),
    {keep_state, maps:without([stream, monitor], Data)};

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
             State,
             #{parameters := Parameters, types := Types})
  when State == query;
       State == execute ->
    {keep_state_and_data,
     nei({process, {Tag, decode(Parameters, lists:zip(Types, Columns))}})};

handle_event(internal, {recv, {notice_response, _} = TM}, query, _) ->
    {keep_state_and_data, nei({process, TM})};

handle_event(internal,
             {recv, {parse_complete, _} = TM},
             parse,
             #{args := [Statement, _]} = Data) ->
    {next_state,
     unsynchronized,
     Data,
     [nei({process, TM}), nei(complete), nei({sync_when_named, Statement})]};

handle_event(internal, {sync_when_named, Name}, _, _) ->
    {keep_state_and_data, [nei(sync) || Name /= <<>>]};

handle_event(internal, {recv, {error_response, _} = TM}, parse, Data) ->
    {next_state,
     unsynchronized,
     Data,
     [nei({process, TM}), nei(complete), nei(sync)]};

handle_event(internal,
             {recv, {bind_complete, _} = Reply},
             bind,
             #{args := [Portal, _, _], cache := Cache} = Data) ->
    ets:delete(Cache, {parameter_description, Portal}),
    ets:delete(Cache, {row_description, Portal}),
    {next_state,
     unsynchronized,
     Data,
     [nei({process, Reply}),
      nei(complete),
      nei({describe_portal, Portal})]};

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
    {next_state, unsynchronized, Data}.


version() ->
    <<Version:32>> = <<(pgmp_config:protocol(major)):16,
                       (pgmp_config:protocol(minor)):16>>,
    Version.


data(_, #{stream := {Ref, PID} = Stream, args := Args}, Data)
  when is_reference(Ref), is_pid(PID) ->
    Data#{stream => Stream,
          args => Args,
          monitor => erlang:monitor(process, PID)};

data(_, #{stream := PID, args := Args}, Data) when is_pid(PID) ->
    Data#{stream => PID,
          args => Args,
          monitor => erlang:monitor(process, PID)};

data({call, From}, #{args := Args}, Data) ->
    Data#{from => From, args => Args, replies => []}.


actions({call, From} = Call, #{stream := _} = Arg, Data) ->
    [{reply, From, ok},
     command(Call, Arg, Data) | post_actions(Call, Arg, Data)];

actions({call, _} = Call, Arg, Data) ->
    [command(Call, Arg, Data) | post_actions(Call, Arg, Data)].


command(_, #{action := Action, args := Args}, _) ->
    nei({Action, Args}).


post_actions(_, #{action := Action}, _) ->
    [nei(flush) || Action == parse
                       orelse Action == describe
                       orelse Action == bind
                       orelse Action == execute].


field_names(Types) ->
    [Name || #{field_name := Name} <- Types].


%% md5(Data) ->
%%     string:lowercase(binary:encode_hex(crypto:hash(md5, Data))).
