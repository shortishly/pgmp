# Docker Compose

To run a local PostgreSQL using [docker compose][docker-compose-file]:

Save the following in `compose.yaml`:

```yaml
services:
  postgres:
    image:
      postgres:15
    environment:
      PGUSER: postgres
      POSTGRES_PASSWORD: postgres
    command:
      -c wal_level=logical
    ports:
      - 5432:5432
    volumes:
      - db:/var/lib/postgresql/data
    healthcheck:
      test: /usr/bin/pg_isready
      interval: 5s
      timeout: 10s
      retries: 5
    pull_policy:
      always
volumes:
  db:
    driver: local
```

Run:

```shell
docker compose up -d
```

[docker-compose-file]: https://docs.docker.com/compose/compose-file/
