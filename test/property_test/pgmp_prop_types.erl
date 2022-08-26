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
    t(?FUNCTION_NAME).


prop_integer() ->
    t(?FUNCTION_NAME).


prop_oid() ->
    t(?FUNCTION_NAME).


prop_oidvector() ->
    t(?FUNCTION_NAME).


prop_integer_array() ->
    t(?FUNCTION_NAME).


prop_bigint() ->
    t(?FUNCTION_NAME).


prop_money() ->
    t(?FUNCTION_NAME).


prop_real() ->
    t(?FUNCTION_NAME).


prop_double_precision() ->
    t(?FUNCTION_NAME).


prop_numeric() ->
    t(?FUNCTION_NAME).


prop_point() ->
    t(?FUNCTION_NAME).


prop_lseg() ->
    t(?FUNCTION_NAME).


prop_box() ->
    t(?FUNCTION_NAME).


prop_line() ->
    t(?FUNCTION_NAME).


prop_bytea() ->
    t(?FUNCTION_NAME).


prop_boolean() ->
    t(?FUNCTION_NAME).


prop_bit_varying() ->
    t(?FUNCTION_NAME).


prop_time() ->
    t(?FUNCTION_NAME).


prop_date() ->
    t(?FUNCTION_NAME).


prop_inet() ->
    t(?FUNCTION_NAME).


prop_uuid() ->
    t(?FUNCTION_NAME).


prop_varchar() ->
    t(?FUNCTION_NAME).


prop_timestamp() ->
    t(?FUNCTION_NAME).


prop_timestamptz() ->
    t(?FUNCTION_NAME).


prop_circle() ->
    t(?FUNCTION_NAME).


prop_polygon() ->
    t(?FUNCTION_NAME).


prop_path() ->
    t(?FUNCTION_NAME).


t(Property) ->
    Type = type(Property),

    numtests(
      ?FORALL(
         Expected,
         generator(Type),
         begin
             Result = pbe(#{sql => ["select $1::", sql_name(Type)], args => [Expected]}),
             ct:log("expected: ~p, result: ~p~n", [Expected, Result]),

             ?WHENFAIL(
                io:format("Expected: ~p, result: ~p~n", [Expected, Result]),
                Result == Expected)
         end)).


generator(smallint) ->
    integer(-32_768, 32_767);

generator(integer) ->
    integer(-2_147_483_648, 2_147_483_647);

generator(numeric) ->
    integer(inf, inf);

generator(integer_array) ->
    list(?FUNCTION_NAME(integer));

generator(bigint) ->
    integer(-9_223_372_036_854_775_808, +9_223_372_036_854_775_807);

generator(money) ->
    integer(-9_223_372_036_854_775_808, +9_223_372_036_854_775_807);

generator(real) ->
    %% real - 4 bytes - 6 decimal digits precision.
    ?LET(Real, float(inf, inf), precision(Real, 6));

generator(double_precision) ->
    %% double - 8 bytes - 15 decimal digits precision
    ?LET(Double, float(inf, inf), precision(Double, 15));

generator(point) ->
    ?LET({X, Y},
         {?FUNCTION_NAME(double_precision),
          ?FUNCTION_NAME(double_precision)},
         #{x => X, y => Y});

generator(polygon) ->
    non_empty(list(?FUNCTION_NAME(point)));

generator(path) ->
    ?LET({Path, Points},
         {oneof([open, closed]), ?FUNCTION_NAME(polygon)},
         #{path => Path, points => Points});

generator(lseg) ->
    {?FUNCTION_NAME(point), ?FUNCTION_NAME(point)};

generator(circle) ->
    ?LET({X, Y, Radius},
         {?FUNCTION_NAME(double_precision),
          ?FUNCTION_NAME(double_precision),
          float(0, inf)},
         #{x => X, y => Y, radius => precision(Radius, 15)});

%% Any two opposite corners can be supplied on input, but the values
%% will be reordered as needed to store the upper right and lower left
%% corners, in that order.
generator(box) ->
    ?LET(
       Box,
       {?FUNCTION_NAME(point), ?FUNCTION_NAME(point)},
       case Box of
           {#{x := HX, y := HY}, #{x := LX, y := LY}} when (HX < LX),
                                     (HY < LY) ->
               {#{x => LX, y => LY}, #{x => HX, y => HY}};

           {#{x := HX, y := HY}, #{x := LX, y := LY}} when (HX < LX) ->
               {#{x => LX, y => HY}, #{x => HX, y => LY}};

           {#{x := HX, y := HY}, #{x := LX, y := LY}} when (HY < LY) ->
               {#{x => HX, y => LY}, #{x => LX, y => HY}};

           {#{x := HX, y := HY}, #{x := LX, y := LY}} ->
               {#{x => HX, y => HY}, #{x => LX, y => LY}}
       end);

generator(line) ->
    ?LET({A, B, C},
         {?FUNCTION_NAME(double_precision),
          ?FUNCTION_NAME(double_precision),
          ?FUNCTION_NAME(double_precision)},
         #{a => A, b => B, c => C});

generator(bytea) ->
    binary();

generator(boolean) ->
    boolean();

generator(bit_varying) ->
    bitstring();

generator(time) ->
    {integer(0, 23), integer(0, 59), integer(0, 59)};

generator(date) ->
    {integer(1, 2100), integer(1, 12), integer(1, 28)};

generator(oid) ->
    integer(0, 4_294_967_295);

generator(inet) ->
    oneof([?FUNCTION_NAME(inet_v4), ?FUNCTION_NAME(inet_v6)]);

generator(inet_v4) ->
    {integer(0, 255), integer(0, 255), integer(0, 255), integer(0, 255)};

generator(inet_v6) ->
    {integer(0, 65_535),
     integer(0, 65_535),
     integer(0, 65_535),
     integer(0, 65_535),
     integer(0, 65_535),
     integer(0, 65_535),
     integer(0, 65_535),
     integer(0, 65_535)};

generator(uuid) ->
    ?LET(
       UUID,
       bitstring(128),
       begin
           <<TimeLow:32, TimeMid:16, TimeHi:16, Clock:16, Node:48>> = UUID,
           iolist_to_binary(
             io_lib:format(
               "~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b",
               [TimeLow, TimeMid, TimeHi, Clock, Node]))
       end);

generator(varchar) ->
    ?LET(Expected, list(alphanumeric()), list_to_binary(Expected));

generator(Type) when Type == timestamptz;
                     Type == timestamp ->
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
         end);

generator(oidvector) ->
    non_empty(list(?FUNCTION_NAME(oid))).


%% prop_bigint -> bigint.
type(Property) ->
    list_to_atom(
      string:prefix(
        atom_to_list(Property),
        "prop_")).


%% double_precision -> "double precision".
sql_name(Type) ->
    string:replace(atom_to_list(Type), "_", " ", all).


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
