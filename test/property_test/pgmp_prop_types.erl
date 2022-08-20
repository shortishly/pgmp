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


-include_lib("proper/include/proper.hrl").


prop_smallint() ->
    numtests(
      ?FORALL(
         Expected,
         integer(-32_768, 32_767),
         begin
             Result = pbe(#{sql => <<"select $1::smallint">>, args => [Expected]}),

             ?WHENFAIL(
                io:format("Expected: ~p, result: ~p~n", [Expected, Result]),
                Result == Expected)
         end)).


prop_integer() ->
    numtests(
      ?FORALL(
         Expected,
         integer(-2_147_483_648, 2_147_483_647),
         begin
             Result = pbe(#{sql => <<"select $1::integer">>, args => [Expected]}),

             ?WHENFAIL(
                io:format("Expected: ~p, result: ~p~n", [Expected, Result]),
                Result == Expected)
         end)).


%% https://www.postgresql.org/docs/8.1/datatype-oid.html
%%
%% The oid type is currently implemented as an unsigned four-byte
%% integer. Therefore, it is not large enough to provide database-wide
%% uniqueness in large databases, or even in large individual
%% tables. So, using a user-created table's OID column as a primary
%% key is discouraged. OIDs are best used only for references to
%% system tables.
%%
prop_oid() ->
    numtests(
      ?FORALL(
         Expected,
         integer(0, 4_294_967_295),
         begin
             Result = pbe(#{sql => <<"select $1::oid">>, args => [Expected]}),

             ?WHENFAIL(
                io:format("Expected: ~p, result: ~p~n", [Expected, Result]),
                Result == Expected)
         end)).

prop_oidvector() ->
    numtests(
      ?FORALL(
         Expected,
         non_empty(list(integer(0, 4_294_967_295))),
         begin
             ct:pal("~p~n", [Expected]),

             Result = pbe(#{sql => <<"select $1::oidvector">>, args => [Expected]}),

             ?WHENFAIL(
                io:format("Expected: ~p, result: ~p~n", [Expected, Result]),
                Result == Expected)
         end)).


prop_integer_array() ->
    numtests(
      ?FORALL(
         Expected,
         list(integer(-2_147_483_648, 2_147_483_647)),
         begin
             Result = pbe(#{sql => <<"select $1::integer array">>, args => [Expected]}),

             ?WHENFAIL(
                io:format("Expected: ~p, result: ~p~n", [Expected, Result]),
                Result == Expected)
         end)).


prop_bigint() ->
    numtests(
      ?FORALL(
         Expected,
         integer(-9_223_372_036_854_775_808, +9_223_372_036_854_775_807),
         begin
             Result = pbe(#{sql => <<"select $1::bigint">>, args => [Expected]}),

             ?WHENFAIL(
                io:format("Expected: ~p, result: ~p~n", [Expected, Result]),
                Result == Expected)
         end)).


prop_numeric() ->
    numtests(
      ?FORALL(
         Expected,
         integer(inf, inf),
         begin
             Result = pbe(#{sql => <<"select $1::numeric">>, args => [Expected]}),

             ?WHENFAIL(
                io:format("Expected: ~p, result: ~p~n", [Expected, Result]),
                Result == Expected)
         end)).

%% real - 4 bytes - 6 decimal digits precision.
prop_real() ->
    numtests(
      ?FORALL(
         Expected,
         ?LET(Expected, float(inf, inf), precision(Expected, 6)),
         begin
             Result = pbe(#{sql => <<"select $1::real">>, args => [Expected]}),

             ?WHENFAIL(
                io:format("expected: ~p, result: ~p~n", [Expected, Result]),
                Result == Expected)
         end)).


%% double - 8 bytes - 15 decimal digits precision
prop_double() ->
    numtests(
      ?FORALL(
         Expected,
         ?LET(Expected, float(inf, inf), precision(Expected, 15)),
         begin
             Result = pbe(#{sql => <<"select $1::double precision">>, args => [Expected]}),

             ?WHENFAIL(
                io:format("expected: ~p, result: ~p~n", [Expected, Result]),
                Result == Expected)
         end)).


precision(Value, Digits) ->
    Decimals = float_to_binary(
                 Value,
                 [{decimals, Digits - trunc(math:ceil(math:log10(trunc(abs(Value)) + 1)))}]),
    case binary:split(Decimals, <<".">>) of
        [_, _] ->
            binary_to_float(Decimals);

        [_] ->
            binary_to_float(<<Decimals/bytes, ".0">>)
    end.


prop_bytea() ->
        numtests(
          ?FORALL(
             Expected,
             binary(),
             begin
                 Result = pbe(#{sql => <<"select $1::bytea">>, args => [Expected]}),

                 ?WHENFAIL(
                    io:format("Expected: ~p, result: ~p~n", [Expected, Result]),
                    Result == Expected)
             end)).


prop_boolean() ->
    numtests(
      ?FORALL(
         Expected,
         boolean(),
         begin
             Result = pbe(#{sql => <<"select $1::boolean">>, args => [Expected]}),

             ?WHENFAIL(
                io:format("Expected: ~p, result: ~p~n", [Expected, Result]),
                Result == Expected)
         end)).


prop_bit_varying() ->
    numtests(
      ?FORALL(
         Expected,
         bitstring(),
         begin
             Result = pbe(#{sql => <<"select $1::bit varying">>, args => [Expected]}),

             ?WHENFAIL(
                io:format("Expected: ~p, result: ~p~n", [Expected, Result]),
                Result == Expected)
         end)).

prop_time() ->
    numtests(
      ?FORALL(
         Expected,
         ?LET({Hour, Minute, Second},
              {integer(0, 23), integer(0, 59), integer(0, 59)},
              {Hour, Minute, Second}),
         begin
             Result = pbe(#{sql => <<"select $1::time">>, args => [Expected]}),

             ?WHENFAIL(
                io:format("Expected: ~p, result: ~p~n", [Expected, Result]),
                Result == Expected)
         end)).


prop_date() ->
    numtests(
      ?FORALL(
         Expected,
         ?LET({Year, Month, Day},
              {integer(1, 2100), integer(1, 12), integer(1, 28)},
              {Year, Month, Day}),
         begin
             Result = pbe(#{sql => <<"select $1::date">>, args => [Expected]}),

             ?WHENFAIL(
                io:format("Expected: ~p, result: ~p~n", [Expected, Result]),
                Result == Expected)
         end)).


prop_timestamp() ->
    numtests(
      ?FORALL(
         Expected,
         ?LET({{Ye, Mo, Da}, {Ho, Mi, Se}},

              {{integer(1, 2100), integer(1, 12), integer(1, 28)},
               {integer(0, 23), integer(0, 59), integer(0, 59)}},

              begin
                  erlang:convert_time_unit(
                    calendar:datetime_to_gregorian_seconds(
                      {{Ye, Mo, Da}, {Ho, Mi, Se}}) -
                        calendar:datetime_to_gregorian_seconds(
                          {{1970, 1, 1}, {0, 0, 0}}),
                  second,
                    microsecond)
              end),
         begin
             Result = pbe(#{sql => <<"select $1::timestamp">>, args => [Expected]}),

             ?WHENFAIL(
                io:format("Expected: ~p, result: ~p~n", [Expected, Result]),
                Result == Expected)
         end)).


prop_timestamptz() ->
    numtests(
      ?FORALL(
         Expected,
         ?LET({{Ye, Mo, Da}, {Ho, Mi, Se}},

              {{integer(1, 2100), integer(1, 12), integer(1, 28)},
               {integer(0, 23), integer(0, 59), integer(0, 59)}},

              begin
                  erlang:convert_time_unit(
                    calendar:datetime_to_gregorian_seconds(
                      {{Ye, Mo, Da}, {Ho, Mi, Se}}) -
                        calendar:datetime_to_gregorian_seconds(
                          {{1970, 1, 1}, {0, 0, 0}}),
                    second,
                    microsecond)
              end),
         begin
             Result = pbe(#{sql => <<"select $1::timestamptz">>, args => [Expected]}),

             ?WHENFAIL(
                io:format("Expected: ~p, result: ~p~n", [Expected, Result]),
                Result == Expected)
         end)).


prop_varchar() ->
    numtests(
      ?FORALL(
         Expected,
         ?LET(Expected, list(alphanumeric()), list_to_binary(Expected)),
         begin
             Result = pbe(#{sql => <<"select $1::varchar">>, args => [Expected]}),

             ?WHENFAIL(
                io:format("Expected: ~p, result: ~p~n", [Expected, Result]),
                Result == Expected)
         end)).


prop_uuid() ->
    numtests(
      ?FORALL(
         Expected,
         ?LET(
            UUID,
            bitstring(128),
            begin
                <<TimeLow:32, TimeMid:16, TimeHi:16, Clock:16, Node:48>> = UUID,
                iolist_to_binary(
                  io_lib:format(
                    "~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b",
                    [TimeLow, TimeMid, TimeHi, Clock, Node]))
            end),
         begin
             Result = pbe(#{sql => <<"select $1::uuid">>, args => [Expected]}),

             ?WHENFAIL(
                io:format("Expected: ~p, result: ~p~n", [Expected, Result]),
                Result == Expected)
         end)).

pbe(Arg) ->
    [{parse_complete, []}] = pgmp_connection_sync:parse(
                               maps:with([sql], Arg)),
    [{bind_complete, []}] = pgmp_connection_sync:bind(
                              maps:with([args], Arg)),

    [{row_description, _},
     {data_row, [Row]},
     {command_complete,
      {select, 1}}] = pgmp_connection_sync:execute(#{}),
    Row.


alphanumeric() ->
    union([integer($a, $z),
           integer($A, $Z),
           integer($0, $9)]).


numtests(Test) ->
    ?FUNCTION_NAME(
       case os:getenv("NUMTESTS") of
           false ->
               100;

           N ->
               erlang:list_to_integer(N)
       end,
       Test).
