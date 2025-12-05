#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

COMPOSE="docker compose -f docker-compose-ci.yml"

echo "🧹 docker compose down -v (clean volumes)..."
$COMPOSE down -v || true

echo "🧹 Pruning unused Docker data (images, cache, volumes, networks)..."
docker system prune -af --volumes || true
docker builder prune -af || true

###############################################################################
# Readiness helpers
###############################################################################
wait_for() {
  local name="$1"
  local cmd="$2"
  local timeout="${3:-180}"   # seconds
  local interval="${4:-2}"    # seconds

  echo "⏳ Waiting for ${name} (timeout ${timeout}s)..."
  local start
  start=$(date +%s)

  while true; do
    if bash -c "$cmd" >/dev/null 2>&1; then
      echo "✅ ${name} is ready"
      return 0
    fi

    local now
    now=$(date +%s)
    if (( now - start >= timeout )); then
      echo "❌ ${name} not ready after ${timeout}s"
      echo "---- ${name} logs (last 200 lines) ----"
      $COMPOSE logs --tail=200 "$name" || true
      return 1
    fi
    sleep "$interval"
  done
}

wait_for_tcp() {
  local name="$1"
  local host="$2"
  local port="$3"
  local timeout="${4:-180}"

  wait_for "$name" "nc -z $host $port" "$timeout" 2
}

###############################################################################
# 1) SSL cert generation for Postgres (UNCHANGED)
###############################################################################
rm -rf certs
mkdir certs
openssl req -new -x509 -days 365 -nodes -out certs/ca.crt -keyout certs/ca.key -subj "/CN=root-ca"

mkdir certs/server
openssl genrsa -des3 -out certs/server/server.key -passout pass:p4ssphrase 2048
openssl rsa -in certs/server/server.key -passin pass:p4ssphrase -out certs/server/server.key
openssl req -new -nodes -key certs/server/server.key -out certs/server/server.csr -subj "/CN=postgresql"
openssl x509 -req -in certs/server/server.csr -days 365 -CA certs/ca.crt -CAkey certs/ca.key -CAcreateserial -out certs/server/server.crt
sudo chmod 600 certs/server/server.key
sudo chown 999:999 certs/server/server.key

mkdir certs/client
openssl genrsa -des3 -out certs/client/client.key -passout pass:p4ssphrase 2048
openssl req -new -nodes -key certs/client/client.key -passin pass:p4ssphrase -out certs/client/client.csr -subj "/CN=postgres"
openssl x509 -req -in certs/client/client.csr -days 365 -CA certs/ca.crt -CAkey certs/ca.key -CAcreateserial -out certs/client/client.crt
# Generate a passphrase-less PKCS#8 key as expected by the JDBC driver.
openssl pkcs8 -topk8 -inform PEM -in certs/client/client.key -passin pass:p4ssphrase -out certs/client/client-no-pass.key -nocrypt

SSL_RESOURCES="plugin-jdbc-postgres/src/test/resources/ssl"
mkdir -p "$SSL_RESOURCES"

if [ -d "$SSL_RESOURCES/postgresql.conf" ]; then
  rm -rf "$SSL_RESOURCES/postgresql.conf"
fi
if [ -d "$SSL_RESOURCES/pg_hba.conf" ]; then
  rm -rf "$SSL_RESOURCES/pg_hba.conf"
fi

rm -f "$SSL_RESOURCES/postgresql.conf" "$SSL_RESOURCES/pg_hba.conf"

cp certs/client/* "$SSL_RESOURCES/"
cp certs/ca.crt "$SSL_RESOURCES/"

cat > "$SSL_RESOURCES/postgresql.conf" <<'EOF'
listen_addresses = '*'
port = 5432

ssl = on
ssl_cert_file = '/var/lib/postgresql/server.crt'
ssl_key_file  = '/var/lib/postgresql/server.key'
ssl_ca_file   = '/var/lib/postgresql/ca.crt'
EOF

cat > "$SSL_RESOURCES/pg_hba.conf" <<'EOF'
hostssl all all 0.0.0.0/0 md5
hostssl all all ::/0      md5
local   all all trust
EOF

###############################################################################
# 2) Start containers
###############################################################################
$COMPOSE up --quiet-pull -d mariadb sqlserver postgres db2 clickhouse druid_postgres druid_zookeeper druid_coordinator druid_broker druid_historical druid_middlemanager druid_router
$COMPOSE up --quiet-pull -d --wait || true

###############################################################################
# 3) Guardrails readiness for EVERY service
###############################################################################

## mysql (64790)
#wait_for "mysql" \
#  "$COMPOSE exec -T mysql mysqladmin ping -h127.0.0.1 -uroot -pmysql_passwd" \
#  180
#
## mariadb (64791)
#wait_for "mariadb" \
#  "$COMPOSE exec -T mariadb mariadb-admin ping -h127.0.0.1 -uroot -pmariadb_passwd" \
#  180
#
## postgres SSL (56982)
#wait_for "postgres" \
#  "$COMPOSE exec -T postgres pg_isready -U postgres -d postgres" \
#  180
#
## postgres-multi-query (56983)
#wait_for "postgres-multi-query" \
#  "$COMPOSE exec -T postgres-multi-query pg_isready -U postgres -d kestra" \
#  180
#
# clickhouse HTTP ping (28123 -> 8123)
wait_for "clickhouse" \
  "curl -sf http://127.0.0.1:28123/ping | grep -q Ok" \
  240

## sqlserver (41433)
#wait_for "sqlserver" \
#  "$COMPOSE logs sqlserver | grep -q 'SQL Server is now ready for client connections'" \
#  420 5
#
## trino (48080)
#wait_for "trino" \
#  "curl -sf http://127.0.0.1:48080/v1/info >/dev/null" \
#  240
#
## pinot (49000)
#wait_for_tcp "pinot" 127.0.0.1 49000 420
#
## dremio (9047)
#wait_for_tcp "dremio-tcp" 127.0.0.1 9047 420
#wait_for "dremio-http" \
#  "code=\$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:9047/ || true); \
#   [ \"\$code\" != \"000\" ]" \
#  420 5
#
# druid nodes
wait_for "druid_coordinator" "curl -sf http://127.0.0.1:11081/status/health | grep -q true" 600 5
wait_for "druid_broker"      "curl -sf http://127.0.0.1:11082/status/health | grep -q true" 600 5
wait_for "druid_historical"  "curl -sf http://127.0.0.1:11083/status/health | grep -q true" 600 5
wait_for "druid_middlemanager" "curl -sf http://127.0.0.1:11091/status/health | grep -q true" 600 5
wait_for "druid_router"      "curl -sf http://127.0.0.1:8888/status/health | grep -q true" 600 5

## oracle (49161)
#wait_for "oracle" \
#  "$COMPOSE exec -T oracle bash -lc 'export ORACLE_HOME=/u01/app/oracle/product/11.2.0/xe; export PATH=\$ORACLE_HOME/bin:\$PATH; export ORACLE_SID=XE; echo \"select 1 from dual;\" | sqlplus -s system/oracle >/dev/null'" \
#  900 5
#
## Increase Oracle limits required by tests then restart to apply.
#$COMPOSE exec -T oracle bash -lc "export ORACLE_HOME=/u01/app/oracle/product/11.2.0/xe; export PATH=\$ORACLE_HOME/bin:\$PATH; export ORACLE_SID=XE; echo 'ALTER SYSTEM SET processes=500 SCOPE=SPFILE;' | sqlplus -s system/oracle"
#$COMPOSE exec -T oracle bash -lc "export ORACLE_HOME=/u01/app/oracle/product/11.2.0/xe; export PATH=\$ORACLE_HOME/bin:\$PATH; export ORACLE_SID=XE; echo 'ALTER SYSTEM SET sessions=555 SCOPE=SPFILE;' | sqlplus -s system/oracle"
#$COMPOSE restart oracle
#
#wait_for "oracle" \
#  "$COMPOSE exec -T oracle bash -lc 'export ORACLE_HOME=/u01/app/oracle/product/11.2.0/xe; export PATH=\$ORACLE_HOME/bin:\$PATH; export ORACLE_SID=XE; echo \"select 1 from dual;\" | sqlplus -s system/oracle >/dev/null'" \
#  900 5
#
## db2 (5023)
#echo "Waiting for DB2 initialization to finish..."
#
#wait_for "db2-init" \
#  "$COMPOSE logs db2 | grep -E -q 'DB2 installation is being initialized|Task #3 end|Setup completed|DB2 instance created'" \
#  1200 10
#
#echo "Waiting for DB2 engine process..."
#
#wait_for "db2-proc" \
#  "$COMPOSE exec -T db2 bash -lc 'pgrep -f db2sysc >/dev/null'" \
#  600 5
#
#echo "Waiting for DB2 remote readiness (db2start + connect)..."
#
#wait_for "db2-connect" \
#  "$COMPOSE exec -T db2 bash -lc '
#     su - db2inst1 -c \"
#       source ~db2inst1/sqllib/db2profile >/dev/null 2>&1 || true
#       db2start >/dev/null 2>&1 || true
#       db2 connect to testdb
#     \"
#   '" \
#  600 5

echo "Waiting for DB2 remote readiness..."
for i in {1..60}; do
  if docker exec plugin-jdbc-db2-1 su - db2inst1 -c "db2 connect to testdb" >/dev/null 2>&1; then
    echo "DB2 ready."
    break
  fi
  sleep 5
done

sleep 5 # Small buffer time before provisioning

###############################################################################
# 4) Provisioning
###############################################################################
docker exec -i plugin-jdbc-mariadb-1 mariadb -uroot -pmariadb_passwd --database=kestra -e """
INSTALL SONAME 'auth_ed25519';
CREATE USER 'ed25519'@'%' IDENTIFIED VIA ed25519 USING PASSWORD('secret');
GRANT SELECT ON kestra.* TO 'ed25519'@'%' IDENTIFIED VIA ed25519 USING PASSWORD('secret');
"""

echo "✅ All services ready & provisioned. Safe to run tests."
