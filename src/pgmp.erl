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


-module(pgmp).


-export([get_env/1]).
-export([priv_consult/1]).
-export([priv_dir/0]).
-export([start/0]).
-export_type([bind_complete/0]).
-export_type([command_complete/0]).
-export_type([data_row/0]).
-export_type([describe_row_description/0]).
-export_type([error_response/0]).
-export_type([int/0]).
-export_type([int16/0]).
-export_type([int32/0]).
-export_type([int64/0]).
-export_type([int8/0]).
-export_type([int_bit_sizes/0]).
-export_type([oid/0]).
-export_type([parameter_description/0]).
-export_type([parameters/0]).
-export_type([parse_complete/0]).
-export_type([portal_suspended/0]).
-export_type([row_description/0]).
-export_type([uint16/0]).
-export_type([uint32/0]).
-export_type([uint64/0]).
-export_type([uint8/0]).

-type int_bit_sizes() :: 8 | 16 | 32 | 64.


-type int() :: {int, int_bit_sizes()}.

-type int8() :: -128..127.
-type int16() :: -32_768..32_767.
-type int32() :: -2_147_483_648..2_147_483_647.
-type int64() :: -9_223_372_036_854_775_808..9_223_372_036_854_775_807.


-type uint8() :: 0..255.
-type uint16() :: 0..65_535.
-type uint32() :: 0..4_294_967_295.
-type uint64() :: 0..18_446_744_073_709_551_615.

-type oid() :: uint32().

-type parameters() :: #{binary() => binary()}.


-type format() :: text | binary.

-type describe_row_description() :: {row_description,
                                     [#{column_number => non_neg_integer(),
                                        field_name => binary(),
                                        format => format(),
                                        table_oid => non_neg_integer(),
                                        type_modifier => integer(),
                                        type_oid => oid(),
                                        type_size => integer()}]}.

-type parameter_description() :: {parameter_description, [oid()]}.

-type column_name() :: binary().
-type row_description() :: {row_description, [column_name()]}.
-type column_value() :: pgmp_data_row:decoded().
-type data_row() :: {data_row, [column_value()]}.
-type command_complete() :: {command_complete, atom() | {atom(), integer()}}.

-type parse_complete() :: {parse_complete, []}.
-type bind_complete() :: {bind_complete, []}.
-type portal_suspended() :: {portal_suspended, []}.

-type error_severity() :: error | fatal | panic.
-type error_response() :: {error, term()}
                        | {error_response,
                           #{code := binary(),
                             file_name := binary(),
                             line := integer(),
                             message := binary(),
                             position := integer(),
                             routine := binary(),
                             severity := error_severity(),
                             severity_localized := binary()}}.


start() ->
    application:ensure_all_started(?MODULE).


priv_dir() ->
    code:priv_dir(?MODULE).


get_env(Par) ->
    application:get_env(?MODULE, Par).


priv_consult(Filename) ->
    phrase_file:consult(filename:join(priv_dir(), Filename)).
