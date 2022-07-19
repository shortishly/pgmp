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
-import(pgmp_codec, [demarshal/1]).
-import(pgmp_codec, [marshal/2]).
-import(pgmp_codec, [prefix_with_size/1]).
-import(pgmp_statem, [nei/1]).


callback_mode() ->
    handle_event_function.


handle_event(info, Msg, _, #{requests := Existing} = Data) ->
    case gen_statem:check_response(Msg, Existing, true) of
        {{reply, ok}, _, Updated} ->
            {keep_state, Data#{requests := Updated}};

        {{error, {Reason, _}}, _From, UpdatedRequests} ->
                {stop, Reason, Data#{requests := UpdatedRequests}}
    end;

handle_event({call, From}, {recv, {Tag, _} = TM}, _, _) ->
    {Decoded, <<>>} = demarshal(TM),
    {keep_state_and_data, [{reply, From, ok}, nei({recv, {Tag, Decoded}})]};

handle_event(internal,
             {send, _} = Request,
             _,
             #{requests := Requests, socket := Socket} = Data) ->
    {keep_state,
     Data#{requests := gen_statem:send_request(
                         Socket,
                         Request,
                         make_ref(),
                         Requests)}};

handle_event(internal, {recv, {authentication, authenticated}}, _, Data) ->
    {next_state, authenticated, Data, pop_callback_module};

handle_event(internal, {recv, {error_response, Errors}}, _, Data) ->
    {next_state,
     startup_failure,
     Data#{errors => Errors},
     pop_callback_module};

handle_event(internal, {md5_password, <<Salt:4/bytes>>}, _, _) ->
    %% src/common/md5_common.c
    %% src/interfaces/libpq/fe-auth.c
    {keep_state_and_data,
     nei({send,
          [<<$p>>,
           prefix_with_size(
             marshal(string,
                     ["md5",
                      md5([md5([pgmp_config:database(password),
                                pgmp_config:database(user)]),
                           Salt])]))]})}.


md5(Data) ->
    string:lowercase(binary:encode_hex(crypto:hash(md5, Data))).
