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


-module(pgmp_uri).


-export([key_words/1]).
-export([parse/1]).


parse(#{query := Query} = URI) ->
    lists:foldl(
      fun
          ({K, V}, A) ->
              Parameter = binary_to_existing_atom(K),

              case lists:member(
                     Parameter,
                     key_words(parameter)) of

                  true ->
                      A#{Parameter => parameter(Parameter, V)};

                  false ->
                      A
              end
      end,
      maps:remove(query, URI),
      uri_string:dissect_query(Query));

parse(#{} = URI) ->
    URI;

parse(URI) when is_binary(URI) ->
    ?FUNCTION_NAME(
       maps:fold(
         fun
             (userinfo, UserInfo, A) ->
                 case string:split(UserInfo, ":") of
                     [User, Password] ->
                         A#{user => User,
                            password => fun
                                            () ->
                                                Password
                                        end};

                     [User] ->
                         A#{user => User}
                 end;

             (scheme, Scheme, A) when Scheme == <<"postgresql">>;
                                      Scheme == <<"postgres">> ->
                 A;

             (_, <<>>, A) ->
                 A;

             (path, <<"/", Name/bytes>>, A) ->
                 A#{database => Name};

             (K, V, A) ->
                 A#{K => V}
         end,
         #{},
         uri_string:parse(URI))).


parameter(port, V) ->
    binary_to_integer(V);

parameter(connect_timeout, V) ->
    binary_to_integer(V);

parameter(keepalives, V) ->
    binary_to_integer(V);

parameter(keepalives_idle, V) ->
    binary_to_integer(V);

parameter(keepalives_interval, V) ->
    binary_to_integer(V);

parameter(keepalives_count, V) ->
    binary_to_integer(V);

parameter(tcp_user_timeout, V) ->
    binary_to_integer(V);

parameter(requiressl, V) ->
    binary_to_integer(V);

parameter(sslcompression, V) ->
    binary_to_integer(V);

parameter(sslsni, V) ->
    binary_to_integer(V);

parameter(_, V) ->
    V.


key_words(parameter) ->
    [host,
     hostaddr,
     port,
     dbname,
     user,
     password,
     passfile,
     channel_binding,
     connect_timeout,
     client_encoding,
     options,
     application_name,
     fallback_application_name,
     keepalives,
     keepalives_idle,
     keepalives_interval,
     keepalives_count,
     tcp_user_timeout,
     replication,
     gssencmode,
     sslmode,
     requiressl,
     sslcompression,
     sslcert,
     sslkey,
     sslpassword,
     sslrootcert,
     sslcrl,
     sslcrldir,
     sslsni,
     requirepeer,
     ssl_min_protocol_version,
     ssl_max_protocol_version,
     krbsrvname,
     gsslib,
     service,
     target_session_attrs].
