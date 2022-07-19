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


-module(pgmp_mm_auth_sasl_tests).


-include_lib("eunit/include/eunit.hrl").


output_length_test_() ->
    lists:map(
      t(fun pgmp_mm_auth_sasl:output_length/1),
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

    #{i := I, r := R, s := S} = pgmp_mm_auth_sasl:decode(ServerFirstMessage),

    SaltedPassword = pgmp_mm_auth_sasl:salted_password(
                       Mechanism,
                       Password,
                       S,
                       I),

    ClientKey = pgmp_mm_auth_sasl:client_key(Mechanism, SaltedPassword),
    StoredKey = pgmp_mm_auth_sasl:stored_key(Mechanism, ClientKey),

    ClientFirstBare = pgmp_mm_auth_sasl:client_first_bare(Username, Nonce),

    ClientFinalWithoutProof = pgmp_mm_auth_sasl:client_final_without_proof(
                                Header,
                                R),

    AuthMessage = pgmp_mm_auth_sasl:auth_message(
                    ClientFirstBare,
                    ServerFirstMessage,
                    ClientFinalWithoutProof),

    ClientSignature = pgmp_mm_auth_sasl:client_signature(
                        Mechanism,
                        StoredKey,
                        AuthMessage),

    ClientProof = pgmp_mm_auth_sasl:client_proof(
                    ClientKey,
                    ClientSignature),

    ?assertEqual(
       <<"c=biws,"
         "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,"
         "p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts=">>,
       iolist_to_binary(
         pgmp_mm_auth_sasl:client_final(Header, R, ClientProof))).



%%
rfc5802_scram_sha256_test() ->
    Header = <<"n,,">>,
    Username = <<"user">>,
    Password = <<"pencil">>,
    Mechanism = <<"SCRAM-SHA-256">>,
    Nonce = <<"rOprNGfwEbeRWgbNEkqO">>,
    ServerFirstMessage = <<"r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF,"
                           "s=W22ZaJ0SNY7soEsUEjb6gQ==,"
                           "i=4096">>,

    #{i := I, r := R, s := S} = pgmp_mm_auth_sasl:decode(ServerFirstMessage),

    SaltedPassword = pgmp_mm_auth_sasl:salted_password(
                       Mechanism,
                       Password,
                       S,
                       I),

    ClientKey = pgmp_mm_auth_sasl:client_key(Mechanism, SaltedPassword),
    StoredKey = pgmp_mm_auth_sasl:stored_key(Mechanism, ClientKey),

    ClientFirstBare = pgmp_mm_auth_sasl:client_first_bare(Username, Nonce),

    ClientFinalWithoutProof = pgmp_mm_auth_sasl:client_final_without_proof(
                                Header,
                                R),

    AuthMessage = pgmp_mm_auth_sasl:auth_message(
                    ClientFirstBare,
                    ServerFirstMessage,
                    ClientFinalWithoutProof),

    ClientSignature = pgmp_mm_auth_sasl:client_signature(
                        Mechanism,
                        StoredKey,
                        AuthMessage),

    ClientProof = pgmp_mm_auth_sasl:client_proof(
                    ClientKey,
                    ClientSignature),

    ?assertEqual(
       <<"c=biws,"
         "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF,"
         "p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=">>,
       iolist_to_binary(
         pgmp_mm_auth_sasl:client_final(Header, R, ClientProof))).
