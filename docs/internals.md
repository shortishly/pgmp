# Internals

The implementation uses some recently introduced features of Erlang/OTP:

- [socket][erlang-org-socket] for all communication
- [send_request][erlang-org-send-request-4] for asynchronous request and response.
- [change_callback_module][erlang-org-change-callback-module] to
  switch between startup, various authentication implementations,
  simple and extended query modes.
- [pbkdf2_hmac][erlang-org-pbkdf2-hmac] in [SCRAM](../src/pgmp_scram.erl)
  authentication.

[erlang-org-change-callback-module]: https://www.erlang.org/doc/man/gen_statem.html#type-action
[erlang-org-pbkdf2-hmac]: https://www.erlang.org/doc/man/crypto.html#pbkdf2_hmac-5
[erlang-org-send-request-4]: https://www.erlang.org/doc/man/gen_statem.html#send_request-4
[erlang-org-socket]: https://www.erlang.org/doc/man/socket.html
