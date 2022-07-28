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


-export([decode/2]).
-export([decode/3]).
-export([decode/5]).
-export([encode/2]).
-export([encode/5]).
-import(pgmp_codec, [marshal/2]).
-import(pgmp_codec, [size_exclusive/1]).


decode(Parameters, TypeValue) ->
    ?FUNCTION_NAME(Parameters, TypeValue, pgmp_types:cache()).

decode(Parameters, TypeValue, Types) ->
    ?FUNCTION_NAME(Parameters, TypeValue, Types, []).

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

decode(_, text, _, #{<<"typname">> := <<"oid">>}, Value) ->
    binary_to_integer(Value);

decode(_,
       binary,
       _,
       #{<<"typname">> := <<"oid">>},
       <<Value:32>>) ->
    Value;

decode(_,
       binary,
       _,
       #{<<"typname">> := <<"bit">>},
       <<Length:32, Data:Length/bits, 0:(8 - Length)>>) ->
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
       #{<<"typname">> := <<"int", _/bytes>>},
       Encoded) ->
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

decode(_,
       _,
       _,
       #{<<"typname">> := Type}, Value)
  when Type == <<"xml">>;
       Type == <<"json">> ->
    (pgmp_config:decode(binary_to_list(Type))):decode(Value);


%% src/backend/utils/adt/arrayfuncs.c#array_recv
decode(Parameters,
       binary = Format,
       Types,
       #{<<"typreceive">> := TypeReceive} = Type,
       Data) when TypeReceive == <<"array_recv">>;
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

decode(_, text, _, #{<<"typname">> := <<"date">>}, <<Ye:4/bytes, "-", Mo:2/bytes, "-", Da:2/bytes>>) ->
    triple(Ye, Mo, Da);

decode(_, binary, _, #{<<"typname">> := <<"date">>}, <<Days:32>>) ->
    calendar:gregorian_days_to_date(calendar:date_to_gregorian_days(epoch_date(pg)) + Days);

decode(_, text, _, #{<<"typname">> := <<"time">>}, <<Ho:2/bytes, ":", Mi:2/bytes, ":", Se:2/bytes>>) ->
    triple(Ho, Mi, Se);

decode(_, binary, _, #{<<"typname">> := <<"time">>}, <<Time:64>>) ->
    calendar:seconds_to_time(erlang:convert_time_unit(Time, microsecond, second));

decode(#{<<"integer_datetimes">> := <<"on">>},
       text,
       _,
       #{<<"typname">> := <<"timestamp">>},
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
         _/bytes>>) ->
    {triple(Ye, Mo, Da), triple(Ho, Mi, Se)};

decode(_, binary, _, #{<<"typname">> := <<"timestamp">>}, <<Encoded:64>>) ->
    calendar:system_time_to_universal_time(
      epoch(pg) + Encoded - epoch(posix),
      microsecond);

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
    Decoded;

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
                               Type == <<"text">> ->
    unicode:characters_to_binary(Encoded);

decode(#{<<"client_encoding">> := <<"SQL_ASCII">>},
       _,
       _,
       #{<<"typname">> := Type},
       <<Encoded/bytes>>) when Type == <<"varchar">>;
                               Type == <<"name">>;
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

decode(Parameters, Format, _TypeCache,Type, Value) ->
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


epoch(System) ->
    erlang:convert_time_unit(
      calendar:datetime_to_gregorian_seconds({epoch_date(System), {0, 0, 0}}),
      second,
      microsecond).

epoch_date(pg) ->
    {2000, 1, 1};

epoch_date(posix) ->
    {1970, 1, 1}.


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
       binary = Format,
       Types,
       #{<<"typsend">> := <<"array_send">>, <<"typelem">> := OID},
       L)
  when is_list(L), OID /= 0 ->
    case maps:find(OID, Types) of
        {ok, Type} ->
            array_send(Parameters, Format, Types, Type, L)
    end;

encode(Parameters,
       binary = Format,
       Types,
       #{<<"typsend">> := <<"oidvectorsend">>, <<"typelem">> := OID},
       L)
  when is_list(L), OID /= 0 ->
    case maps:find(OID, Types) of
        {ok, Type} ->
            vector_send(Parameters, Format, Types, Type, L)
    end;

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
       {{Ye, Mo, Da}, {Ho, Mi, Se}}) ->
    io_lib:format(
      "~4..0b-~2..0b-~2..0b ~2..0b:~2..0b:~2..0b",
      [Ye, Mo, Da, Ho, Mi, Se]);

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
       {Ho, Mi, Se}) ->
    io_lib:format("~2..0b:~2..0b:~2..0b", [Ho, Mi, Se]);

encode(#{<<"integer_datetimes">> := <<"on">>},
       binary,
       _,
       #{<<"typname">> := <<"date">>},
       {_Ye, _Mo, _Da} = Date) ->
    marshal(
      int32,
      calendar:date_to_gregorian_days(Date) - calendar:date_to_gregorian_days(epoch_date(pg)));

encode(_, _, _, #{<<"typname">> := Type}, Value)
  when Type == <<"varchar">>;
       Type == <<"name">>;
       Type == <<"bytea">>;
       Type == <<"json">>;
       Type == <<"xml">>;
       Type == <<"text">> ->
    Value;

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
       <<_:8/bytes, "-", _:4/bytes, "-", _:4/bytes, "-", _:4/bytes, "-", _:12/bytes>> = UUID) ->
    UUID;

encode(_, binary, _, #{<<"typname">> := <<"oid">>}, Value) ->
    <<Value:32>>;

encode(_, text, _, #{<<"typname">> := <<"oid">>}, Value) ->
    integer_to_binary(Value);

encode(_, binary, _, #{<<"typname">> := <<"bit">>}, <<Value/bits>>) ->
    Length = bit_size(Value),
    Padding = byte_size(Value) * 8 - Length,
    <<Length:32, Value/bits, 0:Padding>>;

encode(_, text, _, #{<<"typname">> := <<"bit">>}, <<Value/bits>>) ->
    [integer_to_list(I) || <<I:1>> <= Value];

encode(_, text, _, #{<<"typname">> := <<"float", _/bytes>>}, Value) ->
    numeric(Value);

encode(_, text, _, #{<<"typname">> := <<"int", _/bytes>>}, Value) ->
    integer_to_binary(Value);

encode(_, binary, _, #{<<"typname">> := <<"int", R/bytes>>}, Value) ->
    marshal({int, binary_to_integer(R) * 8}, Value).


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
            (<<Length:32, Value:Length/bytes, R/bytes>>, A) ->
                {R, [decode(Parameters, Format, Types, Type, Value) | A]};

            (<<16#ffffffff:32, R/bytes>>, A) ->
                {R, [null | A]}
        end,
        [],
        Data)).
