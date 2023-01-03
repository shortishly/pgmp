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


-module(pgmp_mm_auth_md5).


-export([callback_mode/0]).
-export([handle_event/4]).
-import(pgmp_codec, [marshal/2]).
-import(pgmp_codec, [size_inclusive/1]).
-import(pgmp_statem, [nei/1]).


callback_mode() ->
    [handle_event_function, state_enter].


handle_event({call, _}, {request, _}, _, _) ->
    {keep_state_and_data, postpone};

handle_event(internal,
             {recv = EventName, {authentication = Tag, authenticated = Type}},
             _,
             Data) ->
    {next_state,
     authenticated,
     Data,
     [pop_callback_module,
      nei({telemetry,
           EventName,
           #{count => 1},
           #{tag => Tag, type => Type}})]};

handle_event(internal,
             {recv = EventName, {error_response = Tag, Errors}},
             _,
             Data) ->
    {next_state,
     startup_failure,
     Data#{errors => Errors},
     [pop_callback_module,
      nei({telemetry, EventName, #{count => 1}, #{tag => Tag}})]};

handle_event(internal,
             {md5_password, <<Salt:4/bytes>>},
             _,
             #{config := #{identity := #{user := User,
                                         password := Password}}}) ->
    %% src/common/md5_common.c
    %% src/interfaces/libpq/fe-auth.c
    {keep_state_and_data,
     nei({send,
          ["p",
           size_inclusive(
             marshal(
               string,
               ["md5", md5([md5([Password(), User()]), Salt])]))]})};

handle_event(EventType, EventContent, State, Data) ->
    pgmp_mm_common:handle_event(EventType,
                                EventContent,
                                State,
                                Data).


md5(Data) ->
    string:lowercase(binary:encode_hex(crypto:hash(md5, Data))).
