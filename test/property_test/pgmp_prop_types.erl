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

-module(pgmp_prop_types).


-export([prop_numeric/0]).
-include_lib("proper/include/proper.hrl").


prop_smallint() ->
    ?FORALL(
       Value,
       integer(-32_768, 32_767),
       begin
           {reply,
            [{parse_complete, []}]} = gen_statem:receive_response(
                                        pgmp_connection:parse(
                                          #{sql => <<"select $1::smallint">>})),

           {reply,
            [{bind_complete, []}]} = gen_statem:receive_response(
                                       pgmp_connection:bind(#{args => [Value]})),

           {reply,
            [{row_description, [_]},
             {data_row, [Result]},
             {command_complete,
              {select, 1}}]} = gen_statem:receive_response(
                                 pgmp_connection:execute(#{})),

           Result == Value
       end).


prop_integer() ->
    ?FORALL(
       Value,
       integer(-2_147_483_648, 2_147_483_647),
       begin
           {reply,
            [{parse_complete, []}]} = gen_statem:receive_response(
                                        pgmp_connection:parse(
                                          #{sql => <<"select $1::integer">>})),

           {reply,
            [{bind_complete, []}]} = gen_statem:receive_response(
                                       pgmp_connection:bind(#{args => [Value]})),

           {reply,
            [{row_description, [_]},
             {data_row, [Result]},
             {command_complete,
              {select, 1}}]} = gen_statem:receive_response(
                                 pgmp_connection:execute(#{})),

           Result == Value
       end).


prop_bigint() ->
    ?FORALL(
       Value,
       integer(-9_223_372_036_854_775_808, +9_223_372_036_854_775_807),
       begin
           {reply,
            [{parse_complete, []}]} = gen_statem:receive_response(
                                        pgmp_connection:parse(
                                          #{sql => <<"select $1::bigint">>})),

           {reply,
            [{bind_complete, []}]} = gen_statem:receive_response(
                                       pgmp_connection:bind(#{args => [Value]})),

           {reply,
            [{row_description, [_]},
             {data_row, [Result]},
             {command_complete,
              {select, 1}}]} = gen_statem:receive_response(
                                 pgmp_connection:execute(#{})),

           Result == Value
       end).


prop_numeric() ->
    ?FORALL(
       Value,
       integer(inf, inf),
       begin
           {reply,
            [{parse_complete, []}]} = gen_statem:receive_response(
                                        pgmp_connection:parse(
                                          #{sql => <<"select $1::numeric">>})),

           {reply,
            [{bind_complete, []}]} = gen_statem:receive_response(
                                       pgmp_connection:bind(#{args => [Value]})),

           {reply,
            [{row_description, [_]},
             {data_row, [Result]},
             {command_complete,
              {select, 1}}]} = gen_statem:receive_response(
                                 pgmp_connection:execute(#{})),

           Result == Value
       end).

%% real - 4 bytes - 6 decimal digits precision
prop_real() ->
    ?FORALL(
       Value,
       float(inf, inf),
       begin
           {reply,
            [{parse_complete, []}]} = gen_statem:receive_response(
                                        pgmp_connection:parse(
                                          #{sql => <<"select $1::real">>})),

           {reply,
            [{bind_complete, []}]} = gen_statem:receive_response(
                                       pgmp_connection:bind(#{args => [Value]})),

           {reply,
            [{row_description, [_]},
             {data_row, [Result]},
             {command_complete,
              {select, 1}}]} = gen_statem:receive_response(
                                 pgmp_connection:execute(#{})),

           ResultDP = precision(Result, 6),
           ValueDP = precision(Value, 6),

           ?WHENFAIL(
              io:format("Value: ~p, result: ~p~n", [ValueDP, ResultDP]),
              ResultDP == ValueDP)
       end).


%% double - 8 bytes - 15 decimal digits precision
prop_double() ->
    ?FORALL(
       Value,
       float(inf, inf),
       begin
           {reply,
            [{parse_complete, []}]} = gen_statem:receive_response(
                                        pgmp_connection:parse(
                                          #{sql => <<"select $1::double precision">>})),

           {reply,
            [{bind_complete, []}]} = gen_statem:receive_response(
                                       pgmp_connection:bind(#{args => [Value]})),

           {reply,
            [{row_description, [_]},
             {data_row, [Result]},
             {command_complete,
              {select, 1}}]} = gen_statem:receive_response(
                                 pgmp_connection:execute(#{})),


           ResultDP = precision(Result, 15),
           ValueDP = precision(Value, 15),

           ?WHENFAIL(
              io:format("Value: ~p, result: ~p~n", [ValueDP, ResultDP]),
              ResultDP == ValueDP)
       end).


precision(Value, Digits) ->
    binary_to_float(
      float_to_binary(
        Value,
        [{decimals, Digits - trunc(math:ceil(math:log10(trunc(abs(Value)) + 1)))}])).
