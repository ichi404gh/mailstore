# Postgres 17 with Patroni from the PGDG repository the official image already has.
FROM postgres:17
RUN apt-get update && apt-get install -y --no-install-recommends patroni python3-etcd python3-psycopg2 curl \
 && rm -rf /var/lib/apt/lists/*
COPY patroni.yml /etc/patroni.yml
USER postgres
ENTRYPOINT ["patroni", "/etc/patroni.yml"]
