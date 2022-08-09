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

-export([auth_message/3]).
-export([callback_mode/0]).
-export([client_final/3]).
-export([client_final_without_proof/2]).
-export([client_first_bare/2]).
-export([client_key/2]).
-export([client_proof/2]).
-export([client_signature/3]).
-export([decode/1]).
-export([h/2]).
-export([handle_event/4]).
-export([hi/4]).
-export([hmac/3]).
-export([output_length/1]).
-export([salted_password/4]).
-export([server_key/2]).
-export([server_signature/3]).
-export([stored_key/2]).
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
    {keep_state_and_data, nei({Action, Encoded, decode(Encoded)})};

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
    SaltedPassword = salted_password(
                       Mechanism,
                       normalize(Password()),
                       Salt,
                       I),

    %% ClientKey       := HMAC(SaltedPassword, "Client Key")
    ClientKey = client_key(Mechanism, SaltedPassword),

    %% StoredKey       := H(ClientKey)
    StoredKey = stored_key(Mechanism, ClientKey),

    %% AuthMessage     := client-first-message-bare + "," +
    %%                    server-first-message + "," +
    %%                    client-final-message-without-proof
    ClientFirstBare = client_first_bare(<<>>, Nonce),

    ClientFinalWithoutProof = client_final_without_proof(
                                Header,
                                R),

    AuthMessage = auth_message(
                    ClientFirstBare,
                    ServerFirstMessage,
                    ClientFinalWithoutProof),

    %% ClientSignature := HMAC(StoredKey, AuthMessage)
    ClientSignature = client_signature(
                        Mechanism,
                        StoredKey,
                        AuthMessage),

    %% ClientProof     := ClientKey XOR ClientSignature
    ClientProof = client_proof(
                    ClientKey,
                    ClientSignature),

    %% ServerKey       := HMAC(SaltedPassword, "Server Key")
    ServerKey = hmac(Mechanism, SaltedPassword, "Server Key"),

    %% ServerSignature := HMAC(ServerKey, AuthMessage)
    ServerSignature = hmac(Mechanism, ServerKey, AuthMessage),

    {keep_state,
     Data#{sasl := SASL#{server => Server,
                         client := Client#{v => ServerSignature}}},
     nei({send,
          ["p",
           size_inclusive(
             [marshal(
                byte,
                pgmp_mm_auth_sasl:client_final(
                  Header,
                  R,
                  ClientProof))])]})};

handle_event(internal,
             {sasl_continue, <<>>, _},
             _,
             #{sasl := #{mechanism := <<"SCRAM-SHA-256">>} = SASL} = Data) ->
    Header = "n,,",
    Nonce = base64:encode(crypto:strong_rand_bytes(16)),
    {keep_state,
     Data#{sasl := SASL#{client => #{header => Header, nonce => Nonce}}},
     nei({send,
          ["p",
           size_inclusive(
             [marshal(
                byte,
                io_lib:fwrite(
                  "~sn=~s,r=~s",
                  [Header, <<>>, Nonce]))])]})};

handle_event(EventType, EventContent, State, Data) ->
    pgmp_mm_common:handle_event(EventType,
                                EventContent,
                                State,
                                Data).


%% SaltedPassword := Hi(Normalize(password), salt, i)
salted_password(Mechanism, Password, Salt, Iterations) ->
    hi(Mechanism, normalize(Password), Salt, Iterations).


%% ClientKey := HMAC(SaltedPassword, "Client Key")
client_key(Mechanism, SaltedPassword) ->
    hmac(Mechanism, SaltedPassword, "Client Key").

%% StoredKey := H(ClientKey)
stored_key(Mechanism, ClientKey) ->
    h(Mechanism, ClientKey).


%% AuthMessage := client-first-message-bare + "," +
%%                server-first-message + "," +
%%                client-final-message-without-proof
auth_message(ClientFirstBare, ServerFirstMessage, ClientFinalWithoutProof) ->
    lists:join(
      ",",
      [ClientFirstBare,
       ServerFirstMessage,
       ClientFinalWithoutProof]).


client_first_bare(Username, Nonce) ->
    io_lib:fwrite("n=~s,r=~s", [Username, Nonce]).


client_final_without_proof(Header, R) ->
    io_lib:fwrite(
      "c=~s,r=~s",
      [base64:encode(Header), R]).


%% ClientSignature := HMAC(StoredKey, AuthMessage)
client_signature(Mechanism, StoredKey, AuthMessage) ->
    hmac(Mechanism, StoredKey, AuthMessage).


%% ClientProof := ClientKey XOR ClientSignature
client_proof(ClientKey, ClientSignature) ->
    crypto:exor(ClientKey, ClientSignature).


%% ServerKey := HMAC(SaltedPassword, "Server Key")
server_key(Mechanism, SaltedPassword) ->
    hmac(Mechanism, SaltedPassword, "Server Key").


%% ServerSignature := HMAC(ServerKey, AuthMessage)
server_signature(Mechanism, ServerKey, AuthMessage) ->
    hmac(Mechanism, ServerKey, AuthMessage).


client_final(Header, R, ClientProof) ->
    lists:join(
      ",",
      [client_final_without_proof(Header, R),
       io_lib:fwrite("p=~s", [base64:encode(ClientProof)])]).


hi(Mechanism, Password, Salt, Iterations) ->
    crypto:pbkdf2_hmac(sub_type(Mechanism),
                       Password,
                       Salt,
                       Iterations,
                       output_length(Mechanism)).

hmac(Mechanism, Key, Data) ->
    crypto:mac(hmac, sub_type(Mechanism), Key, Data).

h(Mechanism, Data) ->
    crypto:hash(sub_type(Mechanism), Data).


sub_type(<<"SCRAM-SHA-1">>) ->
    sha;
sub_type(<<"SCRAM-SHA-256">>) ->
    sha256.


output_length(Mechanism) ->
    byte_size(h(Mechanism, <<>>)).


normalize(X) ->
    X.


decode(Encoded) ->
    maps:map(
      fun
          (K, V) when K == s;
                      K == v ->
              base64:decode(V);

          (i, V) ->
              binary_to_integer(V);

          (_, V) ->
              V
      end,
      lists:foldl(
        fun
            (<<K:1/bytes, "=", V/bytes>>, A) ->
                A#{binary_to_existing_atom(K) => V}
        end,
        #{},
        binary:split(Encoded, <<",">>, [trim_all, global]))).
