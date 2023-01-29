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


-module(pgmp_scram_tests).


-include_lib("eunit/include/eunit.hrl").


output_length_test_() ->
    lists:map(
      t(fun pgmp_scram:output_length/1),
      [{20, <<"SCRAM-SHA-1">>},
       {32, <<"SCRAM-SHA-256">>}]).


t(F) ->
    fun
        ({Expected, Input} = Test) ->
            {nm(Test), ?_assertEqual(Expected, F(Input))}
    end.


nm(Test) ->
    iolist_to_binary(io_lib:fwrite("~p", [Test])).


%% example from:
%% https://datatracker.ietf.org/doc/html/rfc5802
%%
rfc5802_scram_sha1_test() ->
    Header = <<"n,,">>,
    Username = <<"user">>,
    Password = <<"pencil">>,
    Mechanism = <<"SCRAM-SHA-1">>,
    Nonce = <<"fyko+d2lbbFgONRv9qkxdawL">>,
    ServerFirstMessage = <<"r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,"
                           "s=QSXCR+Q6sek8bf92,"
                           "i=4096">>,

    #{i := I, r := R, s := S} = pgmp_scram:decode(ServerFirstMessage),

    SaltedPassword = pgmp_scram:salted_password(
                       Mechanism,
                       Password,
                       S,
                       I),

    ClientKey = pgmp_scram:client_key(Mechanism, SaltedPassword),
    StoredKey = pgmp_scram:stored_key(Mechanism, ClientKey),

    ClientFirstBare = pgmp_scram:client_first_bare(Username, Nonce),

    ClientFinalWithoutProof = pgmp_scram:client_final_without_proof(
                                Header,
                                R),

    AuthMessage = pgmp_scram:auth_message(
                    ClientFirstBare,
                    ServerFirstMessage,
                    ClientFinalWithoutProof),

    ClientSignature = pgmp_scram:client_signature(
                        Mechanism,
                        StoredKey,
                        AuthMessage),

    ClientProof = pgmp_scram:client_proof(
                    ClientKey,
                    ClientSignature),

    ?assertEqual(
       <<"c=biws,"
         "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,"
         "p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts=">>,
       iolist_to_binary(
         pgmp_scram:client_final(Header, R, ClientProof))).




rfc5802_scram_sha256_test() ->
    Header = <<"n,,">>,
    Username = <<>>,
    Password = <<"postgres">>,
    Mechanism = <<"SCRAM-SHA-256">>,
    Nonce = <<"f4xjvKaXgtcHmW4H5Z6CGw==">>,
    ServerFirstMessage = <<"r=f4xjvKaXgtcHmW4H5Z6CGw==8tDuXKyX49H8390ebZMn0ksn,"
                           "s=FYzyO6nFVt34ge1qYjKujQ==,"
                           "i=4096">>,

    #{i := I, r := R, s := S} = pgmp_scram:decode(ServerFirstMessage),

    SaltedPassword = pgmp_scram:salted_password(
                       Mechanism,
                       Password,
                       S,
                       I),

    ClientKey = pgmp_scram:client_key(Mechanism, SaltedPassword),
    StoredKey = pgmp_scram:stored_key(Mechanism, ClientKey),

    ClientFirstBare = pgmp_scram:client_first_bare(Username, Nonce),

    ClientFinalWithoutProof = pgmp_scram:client_final_without_proof(
                                Header,
                                R),

    AuthMessage = pgmp_scram:auth_message(
                    ClientFirstBare,
                    ServerFirstMessage,
                    ClientFinalWithoutProof),

    ClientSignature = pgmp_scram:client_signature(
                        Mechanism,
                        StoredKey,
                        AuthMessage),

    ClientProof = pgmp_scram:client_proof(
                    ClientKey,
                    ClientSignature),

    ?assertEqual(
       <<"c=biws,"
         "r=f4xjvKaXgtcHmW4H5Z6CGw==8tDuXKyX49H8390ebZMn0ksn,"
         "p=c4WWVgk7WTA00JA/9EhqJxXtIRmjNw3NWd0NVVAj7S8=">>,
       iolist_to_binary(
         pgmp_scram:client_final(Header, R, ClientProof))),

    %% ServerKey       := HMAC(SaltedPassword, "Server Key")
    ServerKey = pgmp_scram:server_key(Mechanism, SaltedPassword),

    %% ServerSignature := HMAC(ServerKey, AuthMessage)
    ServerSignature = pgmp_scram:server_signature(Mechanism, ServerKey, AuthMessage),

    ?assertEqual(
       <<139,185,104,111,111,7,103,75,127,181,251,105,26,14,157,
         28,88,131,37,140,189,45,215,124,183,107,35,238,250,38,
         149,40>>,
       ServerSignature).
