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


-module(pgmp_mm_auth_sasl).


-export([callback_mode/0]).
-export([handle_event/4]).
-import(pgmp_codec, [marshal/2]).
-import(pgmp_codec, [size_inclusive/1]).
-import(pgmp_statem, [nei/1]).
-include_lib("kernel/include/logger.hrl").


callback_mode() ->
    [handle_event_function, state_enter].


handle_event(internal, {recv, {authentication, authenticated}}, _, Data) ->
    {next_state, authenticated, Data, pop_callback_module};

handle_event(internal, {recv, {error_response, Errors}}, _, Data) ->
    {next_state,
     startup_failure,
     Data#{errors => Errors},
     pop_callback_module};

%%
%% https://en.wikipedia.org/wiki/Salted_Challenge_Response_Authentication_Mechanism
%%

handle_event(internal,
             {sasl, [<<"SCRAM-SHA-256">> = Mechanism | _]},
             _,
             Data) ->
    {keep_state,
     Data#{sasl => #{mechanism => Mechanism}},
     nei({send,
          ["p",
           size_inclusive(
             [marshal(string, Mechanism), marshal(int32, -1)])]})};

handle_event(internal,
             {recv, {authentication, {Action, Encoded}}},
             _,
             #{sasl := #{mechanism := <<"SCRAM-SHA-256">>}})
  when Action == sasl_continue; Action == sasl_final ->
    {keep_state_and_data, nei({Action, Encoded, pgmp_scram:decode(Encoded)})};

handle_event(internal,
             {sasl_final, _, #{v := V}},
             _,
             #{sasl := #{client := #{v := V}}}) ->
    keep_state_and_data;

handle_event(
  internal,
  {sasl_continue,
   ServerFirstMessage,
   #{r := R, s := Salt, i := I} = Server},
  _,
  #{config := #{identity := #{password := Password}},
    sasl := #{client := #{header := Header, nonce := Nonce} = Client,
              mechanism := <<"SCRAM-SHA-256">> = Mechanism} = SASL} = Data) ->

    %% SaltedPassword  := Hi(Normalize(password), salt, i)
    SaltedPassword = pgmp_scram:salted_password(
                       Mechanism,
                       pgmp_scram:normalize(Password()),
                       Salt,
                       I),

    %% ClientKey       := HMAC(SaltedPassword, "Client Key")
    ClientKey = pgmp_scram:client_key(Mechanism, SaltedPassword),

    %% StoredKey       := H(ClientKey)
    StoredKey = pgmp_scram:stored_key(Mechanism, ClientKey),

    %% AuthMessage     := client-first-message-bare + "," +
    %%                    server-first-message + "," +
    %%                    client-final-message-without-proof
    ClientFirstBare = pgmp_scram:client_first_bare(<<>>, Nonce),

    ClientFinalWithoutProof = pgmp_scram:client_final_without_proof(
                                Header,
                                R),

    AuthMessage = pgmp_scram:auth_message(
                    ClientFirstBare,
                    ServerFirstMessage,
                    ClientFinalWithoutProof),

    %% ClientSignature := HMAC(StoredKey, AuthMessage)
    ClientSignature = pgmp_scram:client_signature(
                        Mechanism,
                        StoredKey,
                        AuthMessage),

    %% ClientProof     := ClientKey XOR ClientSignature
    ClientProof = pgmp_scram:client_proof(
                    ClientKey,
                    ClientSignature),

    %% ServerKey       := HMAC(SaltedPassword, "Server Key")
    ServerKey = pgmp_scram:server_key(Mechanism, SaltedPassword),

    %% ServerSignature := HMAC(ServerKey, AuthMessage)
    ServerSignature = pgmp_scram:server_signature(Mechanism, ServerKey, AuthMessage),

    {keep_state,
     Data#{sasl := SASL#{server => Server,
                         client := Client#{v => ServerSignature}}},
     nei({send,
          ["p",
           size_inclusive(
             [marshal(
                byte,
                pgmp_scram:client_final(
                  Header,
                  R,
                  ClientProof))])]})};

handle_event(internal,
             {sasl_continue, <<>>, _},
             _,
             #{sasl := #{mechanism := <<"SCRAM-SHA-256">>} = SASL} = Data) ->
    Header = "n,,",
    Nonce = base64:encode(crypto:strong_rand_bytes(21)),
    {keep_state,
     Data#{sasl := SASL#{client => #{header => Header, nonce => Nonce}}},
     nei({send,
          ["p",
           size_inclusive(
             [marshal(
                byte,
                [Header,
                 pgmp_scram:client_first_bare(
                   <<>>,
                   Nonce)])])]})};

handle_event(EventType, EventContent, State, Data) ->
    pgmp_mm_common:handle_event(EventType,
                                EventContent,
                                State,
                                Data).
