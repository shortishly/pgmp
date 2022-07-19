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


-export([callback_mode/0]).
-export([handle_event/4]).
-export([init/1]).
-export([cache/0]).
-export([start_link/0]).
-export([start_link/1]).
-export([write_file/1]).
-import(pgmp_statem, [nei/1]).


start_link() ->
    ?FUNCTION_NAME(#{}).

start_link(Arg) ->
    gen_statem:start_link({local, ?MODULE},
                          ?MODULE,
                          [Arg],
                          pgmp_config:options(?MODULE)).

write_file(Filename) ->
    {ok, Header} = file:read_file("HEADER.txt"),

    file:write_file(
      Filename,
      [io_lib:fwrite("%% -*- mode: erlang -*-~n", []),
       Header,
       maps:fold(
         fun
             (K, V, A) ->
                 [io_lib:fwrite("~n~p.~n", [{K, V}]) | A]
         end,
         [],
         cache())]).

cache() ->
    persistent_term:get(?MODULE).


init([Arg]) ->
    case pgmp_config:enabled(?MODULE) of
        true ->
            {ok, unready, Arg#{cache => ets:new(?MODULE, [])}, nei(refresh)};

        false ->
            ignore
    end.


callback_mode() ->
    handle_event_function.


handle_event(internal, refresh, _, _) ->
    {keep_state_and_data, nei({types, hd(pg:get_members(pgmp, pgmp_mm))})};

handle_event(internal, {types, MM}, _, _) ->
    ok = pgmp_mm:request(MM,
           #{action => query,
             args => [<<"select * from pg_catalog.pg_type">>],
             stream => self()}),
    keep_state_and_data;

handle_event(info, {command_complete, {select, _}}, _, #{cache := Cache} = Data) ->
    persistent_term:put(
      ?MODULE,
      ets:foldl(
        fun
            ({{type, OID}, Description}, A) ->
                A#{OID => Description}
        end,
        #{},
        Cache)),

    {next_state, ready, maps:without([columns], Data), hibernate};

handle_event(info,
             {data_row, Columns},
             _,
             #{cache := Cache, columns := Names}) ->
    #{<<"oid">> := OID} = Row = maps:map(
                                  fun
                                      (Column, Value) when Column == <<"oid">>;
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

                                      (_, Value) ->
                                          Value
                                  end,
                                  maps:from_list(lists:zip(Names, Columns))),
    ets:insert(Cache, {{type, OID}, maps:without([oid], Row)}),
    keep_state_and_data;

handle_event(info, {row_description, Columns}, _, Data) ->
    {keep_state, Data#{columns => Columns}}.
