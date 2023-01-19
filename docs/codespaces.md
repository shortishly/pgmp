# Codespaces

You can create a [GitHub Codespace][github-com-codespaces] for pgmp that
has PostgreSQL setup with [instructions from GitHub on creating a
codespace][gihub-com-cacfar]. Some CLI convenience snippets follow.

Firstly, follow the [GitHub CLI installation
instructions][github-com-installation] for your OS.

Clone the pgmp repository:

```shell
gh repo clone shortishly/pgmp
```

Create a codespace for pgmp:

```shell
gh codespace create \
    --repo $(gh repo view \
        --json nameWithOwner \
        --jq .nameWithOwner)
    --branch develop \
    --machine basicLinux32gb
```

Run a secure shell into the codespace:

```shell
gh codespace ssh \
    --codespace $(gh codespace ls \
        --repo $(gh repo view \
            --json nameWithOwner \
            --jq .nameWithOwner)
        --json name \
        --jq '.[].name')
```

Once in the secure shell environment, you can check that
[PostgreSQL][postgresql-org] is running OK:

```shell
docker compose ps
```

To build pgmp, [dialyze][erlang-org-dialyzer] and run the
[unit][erlang-org-eunit-ug] tests:

```shell
make deps app dialyze eunit
```

To run the [common tests][erlang-org-ctug] against the
[PostgreSQL][postgresql-org] already running in the
[codespace][github-com-codespaces]:

```shell
make ct
```

To run an interactive Erlang/OTP shell with the pgmp application running:

```shell
make shell
```

You can then run various commands:

```erlang
1> pgmp_connection_sync:query(#{sql => "select 2 + 2"}).
[{row_description,[<<"?column?">>]},
 {data_row,[4]},
 {command_complete,{select,1}}]
 
 
1> pgmp_connection_sync:parse(#{sql => "select $1::varchar::tsvector"}).
[{parse_complete,[]}]

1> pgmp_connection_sync:bind(#{args => ["a fat cat sat on a mat and ate a fat rat"]}).
[{bind_complete,[]}]

1> pgmp_connection_sync:execute(#{}).
[{row_description,[<<"tsvector">>]},
 {data_row,[#{<<"a">> => #{},<<"and">> => #{},<<"ate">> => #{},
              <<"cat">> => #{},<<"fat">> => #{},<<"mat">> => #{},
              <<"on">> => #{},<<"rat">> => #{},<<"sat">> => #{}}]},
 {command_complete,{select,1}}]

1> pgmp_connection_sync:parse(#{sql => "select $1::varchar::tsquery"}).
[{parse_complete,[]}]

1> pgmp_connection_sync:bind(#{args => ["fat & rat"]}).
[{bind_complete,[]}]

1> pgmp_connection_sync:execute(#{}).
[{row_description,[<<"tsquery">>]},
 {data_row,[['and',
             #{operand => <<"rat">>,prefix => false,weight => 0},
             #{operand => <<"fat">>,prefix => false,weight => 0}]]},
 {command_complete,{select,1}}]

pgmp_connection_sync:query(
  #{sql => "SELECT '{\"reading\": 1.230e-5}'::json,"
           " '{\"reading\": 1.230e-5}'::jsonb"}).
[{row_description,[<<"json">>,<<"jsonb">>]},
 {data_row,[#{<<"reading">> => 1.23e-5},
            #{<<"reading">> => 1.23e-5}]},
 {command_complete,{select,1}}]
```

Once you are finished you can delete the [codespace][github-com-codespaces]
using:

```shell
gh codespace delete \
    --codespace $(gh codespace ls \
        --repo $(gh repo view \
            --json nameWithOwner \
            --jq .nameWithOwner)
        --json name \
        --jq '.[].name')
```

[erlang-org-ctug]: https://www.erlang.org/doc/apps/common_test/users_guide.html
[erlang-org-dialyzer]: https://www.erlang.org/doc/man/dialyzer.html
[erlang-org-eunit-ug]: https://www.erlang.org/doc/apps/eunit/users_guide.html
[gihub-com-cacfar]: https://docs.github.com/en/codespaces/developing-in-codespaces/creating-a-codespace-for-a-repository
[github-com-codespaces]: https://docs.github.com/en/codespaces
[github-com-installation]: https://cli.github.com/manual/installation
[postgresql-org]: https://www.postgresql.org/
