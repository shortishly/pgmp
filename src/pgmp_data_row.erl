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


-module(pgmp_data_row).


-define(NBASE,10_000).
-export([decode/2]).
-export([decode/3]).
-export([decode/5]).
-export([encode/2]).
-export([encode/3]).
-export([encode/5]).
-export([numeric_encode/1]).
-export_type([decoded/0]).
-import(pgmp_codec, [marshal/2]).
-import(pgmp_codec, [size_exclusive/1]).
-include_lib("kernel/include/logger.hrl").


-type format() :: text | binary.
-type type_format() :: #{format := format(), type_oid := pgmp:oid()}.
-type encoded() :: {type_format(), binary() | null}.

-type inet32() :: {pgmp:uint8(), pgmp:uint8(), pgmp:uint8(), pgmp:uint8()}.

-type inet128() :: {pgmp:uint16(),
                    pgmp:uint16(),
                    pgmp:uint16(),
                    pgmp:uint16(),
                    pgmp:uint16(),
                    pgmp:uint16(),
                    pgmp:uint16(),
                    pgmp:uint16()}.

-type decoded() :: boolean()
                 | integer()
                 | bitstring()
                 | binary()
                 | [decoded()]
                 | calendar:date()
                 | calendar:time()
                 | calendar:datetime()
                 | '-Infinity'
                 | 'Infinity'
                 | 'NaN'
                 | float()
                 | inet32()
                 | inet128()
                 | pgmp_geo:point()
                 | pgmp_geo:path()
                 | pgmp_geo:polygon()
                 | pgmp_geo:circle()
                 | pgmp_geo:line()
                 | pgmp_geo:lseg()
                 | pgmp_geo:box().


-spec decode(pgmp:parameters(), [encoded()]) -> [decoded()].

decode(Parameters, TypeValue) ->
    ?FUNCTION_NAME(Parameters, TypeValue, pgmp_types:cache()).


-spec decode(pgmp:parameters(), [encoded()], pgmp_types:cache()) -> [decoded()].

decode(Parameters, TypeValue, Types) ->
    ?FUNCTION_NAME(Parameters, TypeValue, Types, []).


-spec decode(pgmp:parameters(), [encoded()], pgmp_types:cache(), [decoded()]) -> [decoded()].

decode(Parameters, [{_, null} | T], Types, A) ->
    ?FUNCTION_NAME(Parameters, T, Types, [null | A]);

decode(Parameters,
       [{#{format := Format, type_oid := OID}, Value} | T],
       Types,
       A) ->
    case maps:find(OID, Types) of
        {ok, Type} ->
            ?FUNCTION_NAME(
               Parameters,
               T,
               Types,
               [?FUNCTION_NAME(Parameters,
                               Format,
                               Types,
                               Type,
                               Value) | A]);

        error ->
            ?FUNCTION_NAME(Parameters,
                           T,
                           Types,
                           [Value | A])
    end;

decode(_, [], _, A) ->
    lists:reverse(A).


decode(_, binary, _, #{<<"typname">> := <<"bool">>}, <<1>>) ->
    true;

decode(_, text, _, #{<<"typname">> := <<"bool">>}, <<"t">>) ->
    true;

decode(_, binary, _, #{<<"typname">> := <<"bool">>}, <<0>>) ->
    false;

decode(_, text, _, #{<<"typname">> := <<"bool">>}, <<"f">>) ->
    false;

decode(_,
       text,
       _,
       #{<<"typname">> := Name},
       Value) when Name == <<"oid">>;
                   Name == <<"regproc">>;
                   Name == <<"regprocedure">>;
                   Name == <<"regoper">>;
                   Name == <<"regoperator">>;
                   Name == <<"regclass">>;
                   Name == <<"regtype">>;
                   Name == <<"regconfig">>;
                   Name == <<"regdictionary">> ->
    binary_to_integer(Value);

decode(_, text, _, #{<<"typname">> := <<"money">>}, Value) ->
    binary_to_integer(Value);

decode(_,
       binary,
       _,
       #{<<"typname">> := Name},
       <<Value:32>>) when Name == <<"oid">>;
                          Name == <<"regproc">>;
                          Name == <<"regprocedure">>;
                          Name == <<"regoper">>;
                          Name == <<"regoperator">>;
                          Name == <<"regclass">>;
                          Name == <<"regtype">>;
                          Name == <<"regconfig">>;
                          Name == <<"regdictionary">> ->
    Value;

decode(_,
       binary,
       _,
       #{<<"typname">> := Name},
       <<Length:32, Data:Length/bits>>)
    when Length rem 8 == 0,
         Name == <<"bit">>;
         Name == <<"varbit">> ->
    Data;

decode(_,
       binary,
       _,
       #{<<"typname">> := Name},
       <<Length:32, Data:Length/bits, 0:(8 - (Length rem 8))>>)
    when Name == <<"bit">>;
         Name == <<"varbit">> ->
    Data;

decode(_,
       text,
       _,
       #{<<"typname">> := <<"bit">>},
       Encoded) ->
    erlang:list_to_bitstring(
      [<<(binary_to_integer(I)):1>> || <<I:1/bytes>> <= Encoded]);

decode(_,
       text,
       _,
       #{<<"typname">> := Name},
       Encoded)
  when Name == <<"int2">>;
       Name == <<"int4">>;
       Name == <<"int8">> ->
    binary_to_integer(Encoded);

decode(_,
       binary,
       _,
       #{<<"typname">> := <<"uuid">>},
       <<TimeLow:32, TimeMid:16, TimeHi:16, Clock:16, Node:48>>) ->
    iolist_to_binary(
      io_lib:format(
        "~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b",
        [TimeLow, TimeMid, TimeHi, Clock, Node]));

decode(_,
       text,
       _,
       #{<<"typname">> := <<"uuid">>},
       <<_:8/bytes,
         "-",
         _:4/bytes,
         "-",
         _:4/bytes,
         "-",
         _:4/bytes,
         "-",
         _:12/bytes>> = UUID) ->
    UUID;

decode(Parameters,
       binary = Format,
      _,
       #{<<"typname">> := <<"jsonb">> = Type},
       <<1:8, Encoded/bytes>>) ->
    ?LOG_DEBUG(#{parameters => Parameters, format => Format, type => Type, encoded => Encoded}),
    (pgmp_config:codec(binary_to_atom(Type))):?FUNCTION_NAME(Encoded);

decode(Parameters,
       Format,
       _,
       #{<<"typname">> := Type}, Encoded)
  when Type == <<"xml">>;
       Type == <<"jsonb">>;
       Type == <<"json">> ->
    ?LOG_DEBUG(#{parameters => Parameters, format => Format, type => Type, encoded => Encoded}),
    (pgmp_config:codec(binary_to_atom(Type))):?FUNCTION_NAME(Encoded);

%% src/backend/utils/adt/arrayfuncs.c#array_recv
decode(Parameters,
       Format,
       Types,
       #{<<"typreceive">> := TypeReceive} = Type,
       Data) when TypeReceive == <<"array_recv">>;
                  TypeReceive == <<"int2vectorrecv">>;
                  TypeReceive == <<"oidvectorrecv">> ->
    array_recv(Parameters, Format, Types, Type, Data);

decode(_,
       text,
       _,
       #{<<"typname">> := <<"float", _/bytes>>},
       Encoded)
  when Encoded == <<"NaN">>;
       Encoded == <<"Infinity">>;
       Encoded == <<"-Infinity">> ->
    binary_to_atom(Encoded);

decode(_, text, _, #{<<"typname">> := <<"float", _/bytes>>}, Encoded) ->
    numeric(Encoded);

decode(_, text, _, #{<<"typname">> := <<"numeric">>}, Encoded) ->
    numeric(Encoded);


decode(_,
       binary,
       _,
       #{<<"typname">> := <<"numeric">>},
       <<Length:16,
         Weight:16/signed,
         _Sign:16,
         _DScale:16,
         Digits:Length/binary-unit:16>> = Encoded) ->
    lists:foldl(
      numeric_decode(Encoded),
      0,
      lists:zip(
        [Digit || <<Digit:16>> <= Digits],
        lists:seq(Weight, Weight - Length + 1, -1)));

decode(_, text, _, #{<<"typname">> := <<"date">>}, <<Ye:4/bytes, "-", Mo:2/bytes, "-", Da:2/bytes>>) ->
    triple(Ye, Mo, Da);

decode(_, binary, _, #{<<"typname">> := <<"date">>}, <<Days:32/signed>>) ->
    calendar:gregorian_days_to_date(
      calendar:date_to_gregorian_days(pgmp_calendar:epoch_date(pg)) + Days);

decode(_, text, _, #{<<"typname">> := <<"time">>}, <<Ho:2/bytes, ":", Mi:2/bytes, ":", Se:2/bytes>>) ->
    triple(Ho, Mi, Se);

decode(_, binary, _, #{<<"typname">> := <<"time">>}, <<Time:64>>) ->
    calendar:seconds_to_time(erlang:convert_time_unit(Time, microsecond, second));

decode(#{<<"integer_datetimes">> := <<"on">>},
       text,
       _,
       #{<<"typname">> := Name},
       <<Ye:4/bytes,
         "-",
         Mo:2/bytes,
         "-",
         Da:2/bytes,
         " ",
         Ho:2/bytes,
         ":",
         Mi:2/bytes,
         ":",
         Se:2/bytes,
         _/bytes>>)
  when Name == <<"timestamp">>;
       Name == <<"timestamptz">> ->
    {triple(Ye, Mo, Da), triple(Ho, Mi, Se)};

decode(_, binary, _, #{<<"typname">> := Name}, <<Encoded:64/signed>>)
  when Name == <<"timestamp">>;
       Name == <<"timestamptz">> ->
    pgmp_calendar:decode(Encoded);

decode(_, binary, _, #{<<"typname">> := <<"money">>}, <<Decoded:64/signed>>) ->
    Decoded;

decode(_, binary, _, #{<<"typname">> := <<"int", R/bytes>>}, Encoded) ->
    Size = binary_to_integer(R),
    <<Decoded:Size/signed-unit:8>> = Encoded,
    Decoded;

decode(_,
       binary,
       _,
       #{<<"typname">> := <<"float", _/bytes>>},
       <<1:1, ((1 bsl 8) - 1):8, 0:23>>) ->
    '-Infinity';

decode(_,
       binary,
       _,
       #{<<"typname">> := <<"float", _/bytes>>},
       <<0:1, ((1 bsl 8) - 1):8, 0:23>>) ->
    'Infinity';

decode(_,
       binary,
       _,
       #{<<"typname">> := <<"float", _/bytes>>},
       <<_:1, ((1 bsl 8) - 1):8, (1 bsl 22):23>>) ->
    'NaN';

decode(_,
       binary,
       _,
       #{<<"typname">> := <<"float", _/bytes>>},
       <<1:1, ((1 bsl 11) - 1):11, 0:52>>) ->
    '-Infinity';

decode(_,
       binary,
       _,
       #{<<"typname">> := <<"float", _/bytes>>},
       <<0:1, ((1 bsl 11) - 1):11, 0:52>>) ->
    'Infinity';

decode(_,
       binary,
       _,
       #{<<"typname">> := <<"float", _/bytes>>},
       <<_:1, ((1 bsl 11) - 1):11, (1 bsl 51):52>>) ->
    'NaN';

decode(_,
       binary,
       _,
       #{<<"typname">> := <<"float", R/bytes>>},
       Encoded) ->
    Size = binary_to_integer(R),
    <<Decoded:Size/signed-float-unit:8>> = Encoded,
    precision(Decoded, <<"float", R/bytes>>);

decode(_,
       _,
       _,
       #{<<"typname">> := <<"bpchar">>},
       <<Encoded/bytes>>) ->
    Encoded;

decode(#{<<"client_encoding">> := <<"UTF8">>},
       _,
       _,
       #{<<"typname">> := Type},
       <<Encoded/bytes>>) when Type == <<"varchar">>;
                               Type == <<"name">>;
                               Type == <<"regtype">>;
                               Type == <<"text">> ->
    unicode:characters_to_binary(Encoded);

decode(#{<<"client_encoding">> := <<"SQL_ASCII">>},
       _,
       _,
       #{<<"typname">> := Type},
       <<Encoded/bytes>>) when Type == <<"varchar">>;
                               Type == <<"name">>;
                               Type == <<"regtype">>;
                               Type == <<"text">> ->
    unicode:characters_to_binary(Encoded, latin1);

decode(_,
       text,
       _,
       #{<<"typname">> := <<"bytea">>},
       <<"\\x", Encoded/bytes>>) ->
    list_to_binary([binary_to_integer(X, 16) || <<X:2/bytes>> <= Encoded]);

decode(_,
       binary,
       _,
       #{<<"typname">> := <<"bytea">>},
       <<Encoded/bytes>>) ->
    Encoded;

decode(_,
       binary,
       _,
       #{<<"typname">> := <<"tsvector">>},
       Encoded) ->
    pgmp_tsvector:decode(Encoded);

decode(_,
       binary,
       _,
       #{<<"typname">> := <<"tsquery">>},
       Encoded) ->
    pgmp_tsquery:decode(Encoded);

decode(_,
       binary,
       _,
       #{<<"typname">> := <<"macaddr8">>},
       <<Address:8/bytes>>) ->
    Address;

decode(_,
       binary,
       _,
       #{<<"typname">> := <<"macaddr">>},
       <<Address:6/bytes>>) ->
    Address;

decode(_,
       text,
       _,
       #{<<"typname">> := <<"inet">>},
       Encoded) ->
    {ok, Decoded} = inet:parse_address(binary_to_list(Encoded)),
    Decoded;

decode(_,
       binary,
       _,
       #{<<"typname">> := <<"inet">>},
       <<2:8, 32:8, 0:8, Size:8, Encoded:Size/bytes>>) ->
    list_to_tuple([Octet || <<Octet:8>> <= Encoded]);

decode(_,
       binary,
       _,
       #{<<"typname">> := <<"inet">>},
       <<3:8, 128:8, 0:8, Size:8, Encoded:Size/bytes>>) ->
    list_to_tuple([Component || <<Component:16>> <= Encoded]);

decode(_,
       binary,
       _,
       #{<<"typname">> := <<"point">>},
       <<X:8/signed-float-unit:8, Y:8/signed-float-unit:8>>) ->
    #{x => X, y => Y};

decode(_,
       binary,
       _,
       #{<<"typname">> := <<"path">>},
       <<0:8, _:32, Remainder/bytes>>) ->
    #{path => open,
      points => [#{x => X,
                   y => Y} || <<X:8/signed-float-unit:8,
                                Y:8/signed-float-unit:8>> <= Remainder]};

decode(_,
       binary,
       _,
       #{<<"typname">> := <<"path">>},
       <<1:8, _:32, Remainder/bytes>>) ->
    #{path => closed,
      points => [#{x => X,
                   y => Y} || <<X:8/signed-float-unit:8,
                                Y:8/signed-float-unit:8>> <= Remainder]};

decode(_,
       binary,
       _,
       #{<<"typname">> := <<"polygon">>},
       <<_:32, Remainder/bytes>>) ->
    [#{x => X,
       y => Y} || <<X:8/signed-float-unit:8,
                    Y:8/signed-float-unit:8>> <= Remainder];

decode(_,
       binary,
       _,
       #{<<"typname">> := <<"circle">>},
       <<X:8/signed-float-unit:8,
         Y:8/signed-float-unit:8,
         Radius:8/signed-float-unit:8>>) ->
    #{x => X, y => Y, radius => Radius};

decode(_,
       binary,
       _,
       #{<<"typname">> := Type},
       <<X1:8/signed-float-unit:8, Y1:8/signed-float-unit:8,
         X2:8/signed-float-unit:8, Y2:8/signed-float-unit:8>>)
  when Type == <<"lseg">>;
       Type == <<"box">> ->
    {#{x => X1, y => Y1}, #{x => X2, y => Y2}};

decode(_,
       binary,
       _,
       #{<<"typname">> := <<"line">>},
       <<A:8/signed-float-unit:8,
         B:8/signed-float-unit:8,
         C:8/signed-float-unit:8>>) ->
    %% Lines are represented by the linear equation Ax + By + C = 0,
    %% where A and B are not both zero.
    #{a => A, b => B, c => C};

decode(_,
       binary,
       _,
       #{<<"typname">> := <<"geometry">>},
       WKB) ->
    binary:encode_hex(WKB);

decode(_,
       text,
       _,
       #{<<"typname">> := <<"raster">>},
       Raster) ->
    Raster;

decode(_,
       binary,
       _,
       #{<<"typreceive">> := <<"enum_recv">>},
       Enumeration) ->
    Enumeration;

decode(Parameters, Format, _TypeCache, Type, Value) ->
    ?LOG_WARNING(#{parameters => Parameters,
                   format => Format,
                   type => Type,
                   value => Value}),
    #{parameters => Parameters, format => Format, type => Type, value => Value}.


numeric(Value) when is_binary(Value)->
    ?FUNCTION_NAME(
       [fun erlang:binary_to_float/1, fun erlang:binary_to_integer/1], Value);

numeric(Value) when is_number(Value)->
    ?FUNCTION_NAME(
       [fun erlang:float_to_binary/1, fun erlang:integer_to_binary/1], Value).


numeric([Conversion | Conversions], Value) ->
    try
        Conversion(Value)
    catch
        error:badarg ->
            ?FUNCTION_NAME(Conversions, Value)
    end;

numeric([], Value) ->
    Value.


triple(X, Y, Z) ->
    list_to_tuple([binary_to_integer(I) || I <- [X, Y, Z]]).


encode(Parameters, TypeValue) ->
    ?FUNCTION_NAME(Parameters, TypeValue, pgmp_types:cache()).

encode(Parameters, TypeValue, Types) ->
    ?FUNCTION_NAME(Parameters, TypeValue, Types, []).

encode(Parameters, [{_, null} | T], Types, A) ->
    ?FUNCTION_NAME(Parameters, T, Types, [<<-1:32/signed>> | A]);

encode(Parameters,
       [{#{format := Format, type_oid := OID}, Value} | T],
       Types,
       A) ->
    case maps:find(OID, Types) of
        {ok, Type} ->
            ?FUNCTION_NAME(
               Parameters,
               T,
               Types,
               [?FUNCTION_NAME(Parameters,
                               Format,
                               Types,
                               Type,
                               Value) | A]);

        error ->
            ?FUNCTION_NAME(Parameters,
                           T,
                           Types,
                           [Value | A])
    end;

encode(_, [], _, A) ->
    lists:reverse(A).


encode(Parameters,
       Format,
       Types,
       #{<<"typsend">> := <<"array_send">>,
         <<"typelem">> := OID},
       L)
  when is_list(L),
       OID /= 0 ->
    {ok, Type} = maps:find(OID, Types),
    array_send(Parameters, Format, Types, Type, L);

encode(Parameters,
       Format,
       Types,
       #{<<"typsend">> := Send,
         <<"typelem">> := OID},
       L)
  when is_list(L),
       OID /= 0,
       Send == <<"int2vectorsend">>;
       Send == <<"oidvectorsend">> ->
    {ok, Type} = maps:find(OID, Types),
    vector_send(Parameters, Format, Types, Type, L);

encode(_, _, _, #{<<"typname">> := <<"bpchar">>}, Value) ->
    Value;

encode(_, text, _, #{<<"typname">> := <<"bytea">>}, Value) ->
    ["\\x", string:lowercase(binary:encode_hex(Value))];

encode(_, binary, _, #{<<"typname">> := <<"bool">>}, true) ->
    <<1>>;

encode(_, text, _, #{<<"typname">> := <<"bool">>}, true) ->
    "t";

encode(_, binary, _, #{<<"typname">> := <<"bool">>}, false) ->
    <<0>>;

encode(_, text, _, #{<<"typname">> := <<"bool">>}, false) ->
    "f";

encode(#{<<"integer_datetimes">> := <<"on">>},
       text,
       _,
       #{<<"typname">> := <<"timestamp">>},
       {{Ye, Mo, Da}, {Ho, Mi, Se}}) when is_integer(Ye),
                                          is_integer(Mo),
                                          is_integer(Da),
                                          is_integer(Ho),
                                          is_integer(Mi),
                                          is_integer(Se) ->
    io_lib:format(
      "~4..0b-~2..0b-~2..0b ~2..0b:~2..0b:~2..0b",
      [Ye, Mo, Da, Ho, Mi, Se]);

encode(#{<<"integer_datetimes">> := <<"on">>},
       binary,
       _,
       #{<<"typname">> := Name},
       Timestamp) when is_integer(Timestamp),
                       Name == <<"timestamp">>;
                       Name == <<"timestamptz">> ->
    marshal(int64, pgmp_calendar:encode(Timestamp));

encode(#{<<"integer_datetimes">> := <<"on">>},
       text,
       _,
       #{<<"typname">> := <<"date">>},
       {Ye, Mo, Da}) ->
    io_lib:format(
      "~4..0b-~2..0b-~2..0b",
      [Ye, Mo, Da]);


encode(#{<<"integer_datetimes">> := <<"on">>},
       text,
       _,
       #{<<"typname">> := <<"time">>},
       {Ho, Mi, Se}) when is_integer(Ho),
                          is_integer(Mi),
                          is_integer(Se) ->
    io_lib:format(
      "~2..0b:~2..0b:~2..0b",
      [Ho, Mi, Se]);

encode(#{<<"integer_datetimes">> := <<"on">>},
       binary,
       _,
       #{<<"typname">> := <<"time">>},
       {Ho, Mi, Se}  = Time) when is_integer(Ho),
                                  is_integer(Mi),
                                  is_integer(Se) ->
    marshal(
      int64,
      erlang:convert_time_unit(
        calendar:time_to_seconds(Time), second, microsecond));

encode(#{<<"integer_datetimes">> := <<"on">>},
       binary,
       _,
       #{<<"typname">> := <<"date">>},
       {_Ye, _Mo, _Da} = Date) ->
    marshal(
      int32,
      calendar:date_to_gregorian_days(Date) -
          calendar:date_to_gregorian_days(pgmp_calendar:epoch_date(pg)));

encode(_,
       binary,
       _,
       #{<<"typname">> := <<"jsonb">> = Type}, Value) ->
    [1, (pgmp_config:codec(binary_to_atom(Type))):?FUNCTION_NAME(Value)];

encode(_,
       _,
       _,
       #{<<"typname">> := Type}, Value)
  when Type == <<"json">>;
       Type == <<"jsonb">>;
       Type == <<"xml">> ->
    (pgmp_config:codec(binary_to_atom(Type))):?FUNCTION_NAME(Value);

encode(_,
       binary,
       _,
       #{<<"typname">> := <<"uuid">>},
       <<TimeLow:8/bytes,
         "-",
         TimeMid:4/bytes,
         "-",
         TimeHi:4/bytes,
         "-",
         Clock:4/bytes,
         "-",
         Node:12/bytes>>) ->
    lists:map(
      fun binary:decode_hex/1,
      [TimeLow, TimeMid, TimeHi, Clock, Node]);

encode(_,
       text,
       _,
       #{<<"typname">> := <<"uuid">>},
       <<_:8/bytes,
         "-",
         _:4/bytes,
         "-",
         _:4/bytes,
         "-",
         _:4/bytes,
         "-",
         _:12/bytes>> = UUID) ->
    UUID;

encode(_,
       binary,
       _,
       #{<<"typname">> := Name},
       Value) when Name == <<"oid">>;
                   Name == <<"regproc">>;
                   Name == <<"regprocedure">>;
                   Name == <<"regoper">>;
                   Name == <<"regoperator">>;
                   Name == <<"regclass">>;
                   Name == <<"regtype">>;
                   Name == <<"regconfig">>;
                   Name == <<"regdictionary">> ->
    <<Value:32>>;

encode(_,
       text,
       _,
       #{<<"typname">> := Name},
       Value) when Name == <<"oid">>;
                   Name == <<"regproc">>;
                   Name == <<"regprocedure">>;
                   Name == <<"regoper">>;
                   Name == <<"regoperator">>;
                   Name == <<"regclass">>;
                   Name == <<"regtype">>;
                   Name == <<"regconfig">>;
                   Name == <<"regdictionary">> ->
    integer_to_binary(Value);

encode(_, binary, _, #{<<"typname">> := Name}, <<Value/bits>>)
  when Name == <<"bit">>;
       Name == <<"varbit">>  ->
    Length = bit_size(Value),
    Padding = byte_size(Value) * 8 - Length,
    <<Length:32, Value/bits, 0:Padding>>;

encode(_, text, _, #{<<"typname">> := <<"bit">>}, <<Value/bits>>) ->
    [integer_to_list(I) || <<I:1>> <= Value];

encode(_, text, _, #{<<"typname">> := <<"float", _/bytes>>}, Value) ->
    numeric(Value);

encode(_, binary, _, #{<<"typname">> := <<"float", R/bytes>>}, Value) ->
    Size = binary_to_integer(R),
    <<(precision(Value, <<"float", R/bytes>>)):Size/signed-float-unit:8>>;

encode(_, text, _, #{<<"typname">> := Name}, Value)
  when Name == <<"int2">>;
       Name == <<"int4">>;
       Name == <<"int8">> ->
    integer_to_binary(Value);

encode(_, binary, _, #{<<"typname">> := <<"numeric">>}, Value) ->
    {Weight, Digits} = numeric_encode(abs(Value)),
    [<<(length(Digits)):16,
       Weight:16,
       (case Value of
           Positive when  Positive >= 0 ->
               0;

           _ ->
               16384
       end):16,
       0:16>>,
     [<<Digit:16>> || Digit <- Digits]];

encode(_,
       binary,
       _,
       #{<<"typname">> := <<"tsvector">>},
       Value) ->
    pgmp_tsvector:encode(Value);

encode(_,
       binary,
       _,
       #{<<"typname">> := <<"tsquery">>},
       Value) ->
    pgmp_tsquery:encode(Value);

encode(_,
       binary,
       _,
       #{<<"typname">> := <<"macaddr8">>},
       <<Value:8/bytes>>) ->
    Value;

encode(_,
       binary,
       _,
       #{<<"typname">> := <<"macaddr">>},
       <<Value:6/bytes>>) ->
    Value;

encode(_,
       binary,
       _,
       #{<<"typname">> := <<"inet">>},
       {_, _, _, _} = Value) ->
    [<<2:8, 32:8, 0:8, 4:8>>, tuple_to_list(Value)];

encode(_,
       binary,
       _,
       #{<<"typname">> := <<"inet">>},
       {_, _, _, _, _, _, _, _} = Value) ->
    [<<3:8,
       128:8,
       0:8,
       16:8>>,
     [<<Component:16>> || Component <- tuple_to_list(Value)]];

encode(_, text, _, #{<<"typname">> := <<"inet">>}, Value) ->
    Value;

encode(_,
       binary,
       _,
       #{<<"typname">> := <<"point">>},
       #{x := X, y := Y}) ->
    <<X:8/float-unit:8, Y:8/float-unit:8>>;

encode(_,
       binary,
       _,
       #{<<"typname">> := <<"path">>},
       #{path := open, points := Points}) when is_list(Points) ->
    [<<0:8, (length(Points)):32>>,
     [<<X:8/signed-float-unit:8,
        Y:8/signed-float-unit:8>> || #{x := X,
                                       y := Y} <- Points]];

encode(_,
       binary,
       _,
       #{<<"typname">> := <<"path">>},
       #{path := closed, points := Points}) when is_list(Points) ->
    [<<1:8, (length(Points)):32>>,
     [<<X:8/signed-float-unit:8,
        Y:8/signed-float-unit:8>> || #{x := X,
                                       y := Y} <- Points]];

encode(_,
       binary,
       _,
       #{<<"typname">> := <<"polygon">>},
       Points) when is_list(Points) ->
    [<<(length(Points)):32>>,
     [<<X:8/signed-float-unit:8,
        Y:8/signed-float-unit:8>> || #{x := X,
                                       y := Y} <- Points]];

encode(_,
       binary,
       _,
       #{<<"typname">> := <<"circle">>},
       #{x := X, y := Y, radius := Radius}) ->
    <<X:8/float-unit:8, Y:8/float-unit:8, Radius:8/float-unit:8>>;

encode(_,
       binary,
       _,
       #{<<"typname">> := Type},
       {#{x := X1, y := Y1}, #{x := X2, y := Y2}})
  when Type == <<"lseg">>;
       Type == <<"box">> ->
    <<X1:8/float-unit:8, Y1:8/float-unit:8,
      X2:8/float-unit:8, Y2:8/float-unit:8>>;

encode(_,
       binary,
       _,
       #{<<"typname">> := <<"line">>},
       #{a := A, b := B, c := C})
  when A /= 0, B /= 0 ->
    %% Lines are represented by the linear equation Ax + By + C = 0,
    %% where A and B are not both zero.
    <<A:8/float-unit:8, B:8/float-unit:8, C:8/float-unit:8>>;

encode(_, binary, _, #{<<"typname">> := <<"money">>}, Value) when is_integer(Value) ->
    marshal({int, 64}, Value);

encode(_, binary, _, #{<<"typname">> := Name}, Value)
  when Name == <<"int2">>;
       Name == <<"int4">>;
       Name == <<"int8">> ->
    <<"int", R/bytes>> = Name,
    marshal({int, binary_to_integer(R) * 8}, Value);

encode(_, _, _, #{<<"typname">> := Type}, Value)
  when Type == <<"varchar">>;
       Type == <<"name">>;
       Type == <<"bytea">>;
       Type == <<"macaddr8">>;
       Type == <<"macaddr">>;
       Type == <<"text">> ->
    Value;

encode(_, text, _, _, Value) ->
    Value.


vector_send(
  Parameters,
  text = Format,
  Types,
  Type,
  L) ->
    lists:join(
      " ",
      [encode(Parameters, Format, Types, Type, V) || V <- L]);


vector_send(_, binary, _, #{<<"oid">> := OID}, []) ->
    <<1:32, 0:32, OID:32>>;

%% must be 1-D, 0-based with no nulls
vector_send(
  Parameters,
  binary = Format,
  Types,
  #{<<"oid">> := OID} = Type,
  L) ->
    [<<1:32,
       0:32,
       OID:32,
       (length(L)):32,
       0:32>>,
     [size_exclusive(encode(Parameters, Format, Types, Type, V)) || V <- L]].


array_send(_, binary, _, #{<<"oid">> := OID}, []) ->
    <<0:32, 0:32, OID:32>>;

array_send(
  Parameters,
  binary = Format,
  Types,
  #{<<"oid">> := OID} = Type,
  L) ->
    [<<1:32,
       (null_bitmap(L)):32,
       OID:32,
       (length(L)):32,
       1:32>>,
     lists:map(
       fun
           (null) ->
               marshal(int32, -1);

           (V) ->
               size_exclusive(encode(Parameters, Format, Types, Type, V))
       end,
       L)].


null_bitmap(L) ->
    case lists:any(
           fun
               (Value) ->
                   Value == null
           end,
           L) of
        false ->
            0;

        true ->
            1
    end.


array_recv(_, text, _, _, <<>>) ->
    [];

array_recv(Parameters,
           text = Format,
           Types,
           #{<<"typelem">> := OID},
           Values) ->
    {ok, Type} =  maps:find(OID, Types),
    [decode(Parameters,
            Format,
            Types,
            Type,
            Value) || Value <- binary:split(Values, <<" ">>, [global])];

array_recv(_, binary, _, _, <<0:32, 0:32, _:32>>) ->
    [];

array_recv(Parameters,
           binary = Format,
           Types,
           _,
           <<1:32, %% NDim
             0:32,
             OID:32,
             _Dimensions:32,
             _LowerBnds:32,
             Data/bytes>>) ->
    {ok, Type} =  maps:find(OID, Types),
    [decode(Parameters,
            Format,
            Types,
            Type,
            Value) || <<Length:32, Value:Length/bytes>> <= Data];

array_recv(Parameters,
           binary = Format,
           Types,
           _,
           <<1:32, %% NDim
             1:32,
             OID:32,
             _Dimensions:32,
             _LowerBnds:32,
             Data/bytes>>) ->
    {ok, Type} =  maps:find(OID, Types),
    lists:reverse(
      pgmp_binary:foldl(
        fun
            (<<-1:32/signed, R/bytes>>, A) ->
                {R, [null | A]};

            (<<Length:32, Value:Length/bytes, R/bytes>>, A) ->
                {R, [decode(Parameters, Format, Types, Type, Value) | A]}
        end,
        [],
        Data)).


numeric_decode(<<Length:16,
                 _:16/signed,
                 0:16,
                 _:16,
                 _:Length/binary-unit:16>>) ->
    fun
        ({Digit, Weight}, A) ->
            ?LOG_DEBUG(#{digit => Digit, weight => Weight, a => A}),
            (pow(?NBASE, Weight) * Digit) + A
    end;

numeric_decode(<<Length:16,
                 _:16/signed,
                 16384:16,
                 _:16,
                 _:Length/binary-unit:16>>) ->
    fun
        ({Digit, Weight}, A) ->
            ?LOG_DEBUG(#{digit => Digit, weight => Weight, a => A}),
            A - (pow(?NBASE, Weight) * Digit)
    end.


pow(X, N) when is_integer(X), is_integer(N), N > 1 ->
    ?FUNCTION_NAME(1, X, N);
pow(_, 0) ->
    1;
pow(X, 1) ->
    X;
pow(X, N) ->
    math:pow(X, N).


pow(Y, _, 0) ->
    Y;
pow(Y, X, N) when N rem 2 == 0 ->
    ?FUNCTION_NAME(Y, X * X, N div 2);
pow(Y, X, N) ->
    ?FUNCTION_NAME(X * Y, X * X, (N - 1) div 2).


numeric_encode(Value) when is_integer(Value) ->
    ?FUNCTION_NAME(Value, [], 0).

%%     ?FUNCTION_NAME(Value, [], 0);
%% numeric_encode(Value) when is_float(Value) ->
%%     [Integer, Decimal] = [binary_to_integer(Part) || Part <- binary:split(
%%                                                                float_to_binary(
%%                                                                  Value,
%%                                                                  [{decimals, 4}]),
%%                                                                <<".">>)],

%%     ?LOG_DEBUG(#{value => Value,
%%                 integer => Integer,
%%                 decimal => Decimal}),

%%     {Weight, Digits} = ?FUNCTION_NAME(Integer, [], 0),

%%     ?LOG_DEBUG(#{value => Value,
%%                  weight => Weight,
%%                  digits => Digits,
%%                  integer => Integer,
%%                  decimal => Decimal}),

%%     ?FUNCTION_NAME(Decimal, Digits, Weight).


numeric_encode(0, Digits, Weight) ->
    {Weight - 1, Digits};

numeric_encode(Value, [] = Digits, Weight) ->
    case Value rem ?NBASE of
        0 ->
            ?FUNCTION_NAME(Value div ?NBASE, Digits, Weight + 1);

        Remainder ->
            ?FUNCTION_NAME(Value div ?NBASE, [Remainder | Digits], Weight + 1)
    end;

numeric_encode(Value, Digits, Weight) ->
    ?FUNCTION_NAME(Value div ?NBASE, [Value rem ?NBASE | Digits], Weight + 1).


precision(Value, <<"float8">>) ->
    ?FUNCTION_NAME(Value, 15);

precision(Value, <<"float4">>) ->
    ?FUNCTION_NAME(Value, 6);

precision(Value, Digits) when is_integer(Value) ->
    ?FUNCTION_NAME(float(Value), Digits);

precision(Value, Digits) when is_float(Value),
                              is_integer(Digits) ->
    Decimals = Digits - trunc(math:ceil(math:log10(trunc(abs(Value)) + 1))),
    binary_to_float(
      iolist_to_binary(
        io_lib:fwrite("~.*f", [Decimals, Value]))).
