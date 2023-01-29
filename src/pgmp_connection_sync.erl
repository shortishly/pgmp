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


-module(pgmp_connection_sync).


-export([bind/1]).
-export([describe/1]).
-export([execute/1]).
-export([parse/1]).
-export([query/1]).
-export([sync/1]).


-type bind_request() :: #{name => iodata(),
                          portal => iodata(),
                          args => [any()],
                          parameter => binary | text,
                          result => binary | text}.
-type bind_response() :: pgmp:bind_complete()
                       | pgmp:error_response().
-spec bind(bind_request()) -> [bind_response(), ...].

bind(Arg) ->
    receive_response(?FUNCTION_NAME, Arg).

-type statement() :: $S.
-type portal() :: $P.
-type describe_type() :: statement() | portal().
-type describe_request() :: #{type := describe_type(), name => iodata()}.
-type describe_response() :: pgmp:describe_row_description()
                           | pgmp:parameter_description()
                           | pgmp:error_response().
-spec describe(describe_request()) -> [describe_response(), ...].

describe(Arg) ->
    receive_response(?FUNCTION_NAME, Arg).


-type execute_request() :: #{portal => iodata(), max_rows => non_neg_integer()}.
-type execute_response() :: query_response()
                          | pgmp:portal_suspended().
-spec execute(execute_request()) -> [execute_response(), ...].

execute(Arg) ->
    receive_response(?FUNCTION_NAME, Arg).


-type parse_request() :: #{name => iodata(), sql := iodata()}.
-type parse_response() :: pgmp:parse_complete()
                        | pgmp:error_response().
-spec parse(parse_request()) -> [parse_response(), ...].

parse(Arg) ->
    receive_response(?FUNCTION_NAME, Arg).


-type query_request() :: #{sql := iodata()}.
-type query_response() :: pgmp:row_description()
                        | pgmp:data_row()
                        | pgmp:command_complete()
                        | pgmp:error_response().
-spec query(query_request()) -> [query_response(), ...].

query(Arg) ->
    receive_response(?FUNCTION_NAME, Arg).


-type sync_request() :: #{}.
-type sync_response() :: any().
-spec sync(sync_request()) -> sync_response().

sync(Arg) ->
    receive_response(?FUNCTION_NAME, Arg).


receive_response(Function, Arg) ->
    case gen_statem:receive_response(pgmp_connection:Function(Arg)) of
        {reply, Reply} ->
            Reply;

        {error, _} = Error ->
            Error
    end.
