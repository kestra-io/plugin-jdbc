#!/usr/bin/env bash
# Redshift's JDBC driver only recognises SUPER by its fixed OID (4000).
# PostgreSQL only lets you choose a type OID in binary-upgrade mode, so restart the init server with -b.
set -euo pipefail
pg_ctl -D "$PGDATA" -m fast -w stop
pg_ctl -D "$PGDATA" -o "-b -c listen_addresses=''" -w start
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /docker-entrypoint-initdb.d/super.sql.in
pg_ctl -D "$PGDATA" -m fast -w stop
pg_ctl -D "$PGDATA" -o "-c listen_addresses=''" -w start
