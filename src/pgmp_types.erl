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


-module(pgmp_types).


%% -export([write_file/1]).
-behaviour(gen_statem).
-export([cache/1]).
-export([callback_mode/0]).
-export([handle_event/4]).
-export([init/1]).
-export([server_ref/1]).
-export([start_link/1]).
-export([when_ready/1]).
-export_type([cache/0]).
-import(pgmp_statem, [nei/1]).
-include("pgmp_types.hrl").


-type cache() :: #{pgmp:oid() => #{binary() => any()}}.


start_link(Arg) ->
    gen_statem:start_link(
      {local, server_ref(Arg)},
      ?MODULE,
      [Arg],
      envy_gen:options(?MODULE)).


-spec server_ref(pgmp_dbs_sup:db()) -> gen_statem:server_ref().

server_ref(#{application_name := ApplicationName}) ->
    pgmp_util:snake_case([binary_to_list(ApplicationName), ?MODULE]).


when_ready(Arg) ->
    send_request(
      maps:merge(
        Arg,
        #{request => ?FUNCTION_NAME})).


send_request(#{label := _} = Arg) ->
    pgmp_statem:send_request(Arg);

send_request(Arg) ->
    pgmp_statem:send_request(Arg#{label => ?MODULE}).


%% write_file(Filename) ->
%%     {ok, Header} = file:read_file("HEADER.txt"),

%%     file:write_file(
%%       Filename,
%%       [io_lib:fwrite("%% -*- mode: erlang -*-~n", []),
%%        Header,
%%        maps:fold(
%%          fun
%%              (K, V, A) ->
%%                  [io_lib:fwrite("~n~p.~n", [{K, V}]) | A]
%%          end,
%%          [],
%%          cache())]).


-spec cache(pgmp_dbs_sup:db()) -> cache().

cache(Arg) ->
    persistent_term:get(persistent_term_name(Arg)).


persistent_term_name(#{application_name := ApplicationName}) ->
    pgmp_util:snake_case([binary_to_list(ApplicationName), ?MODULE]).


init([Arg]) ->
    case pgmp_config:enabled(?MODULE) of
        true ->
            {ok,
             unready,
             #{cache => ets:new(?MODULE, []),
               config => Arg,
               requests => gen_statem:reqids_new()},
             nei(refresh)};

        false ->
            ignore
    end.


callback_mode() ->
    handle_event_function.


handle_event({call, _}, when_ready, unready, _) ->
    {keep_state_and_data, postpone};

handle_event({call, From}, when_ready, ready, _) ->
    {keep_state_and_data, {reply, From, ready}};

handle_event(internal,
             refresh,
             _,
             #{config := Config, requests := Requests} = Data) ->
    {keep_state,
     Data#{requests := pgmp_connection:query(
                         #{sql=> <<?TYPE_SQL>>,
                           label => types,
                           server_ref => pgmp_connection:server_ref(Config),
                           requests => Requests})}};

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

handle_event(internal,
             {response, #{label := types, reply := Replies}},
             _,
             _) ->
    {keep_state_and_data, [nei(Reply) || Reply <- Replies]};

handle_event(internal,
             {command_complete, {select, _}},
             _,
             #{config := Config, cache := Cache} = Data) ->
    persistent_term:put(
      persistent_term_name(Config),
      ets:foldl(
        fun
            ({{type, OID}, Description}, A) ->
                A#{OID => Description}
        end,
        #{},
        Cache)),

    {next_state, ready, maps:without([columns], Data), hibernate};

handle_event(internal,
             {data_row, Columns},
             _,
             #{cache := Cache, columns := Names}) ->
    #{<<"oid">> := OID} = Row = maps:map(
                                  fun data_row/2,
                                  maps:from_list(
                                    lists:zip(Names, Columns))),
    ets:insert(Cache, {{type, OID}, maps:without([oid], Row)}),
    keep_state_and_data;

handle_event(internal, {row_description, Columns}, _, Data) ->
    {keep_state, Data#{columns => Columns}}.


data_row(Column, Value) when Column == <<"oid">>;
                             Column == <<"typnamespace">>;
                             Column == <<"typowner">>;
                             Column == <<"typlen">>;
                             Column == <<"typrelid">>;
                             Column == <<"typelem">>;
                             Column == <<"typarray">>;
                             Column == <<"typbasetype">>;
                             Column == <<"typtypmod">>;
                             Column == <<"typndims">>;
                             Column == <<"typcollation">> ->
    binary_to_integer(Value);

data_row(_, Value) ->
    Value.
