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


-module(pgmp_scram).


%%
%% https://en.wikipedia.org/wiki/Salted_Challenge_Response_Authentication_Mechanism
%%


-export([auth_message/3]).
-export([client_final/3]).
-export([client_final_without_proof/2]).
-export([client_first_bare/2]).
-export([client_key/2]).
-export([client_proof/2]).
-export([client_signature/3]).
-export([decode/1]).
-export([normalize/1]).
-export([output_length/1]).
-export([salted_password/4]).
-export([server_key/2]).
-export([server_signature/3]).
-export([stored_key/2]).


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
