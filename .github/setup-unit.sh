set -euo pipefail

echo "🧹 docker compose down -v (clean volumes)..."
docker compose -f docker-compose-ci.yml down -v || true

echo "🧹 Pruning unused Docker data (images, cache, volumes, networks)..."
docker system prune -af --volumes || true
docker builder prune -af || true

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
openssl rsa -in certs/client/client.key -passin pass:p4ssphrase -out certs/client/client-no-pass.key
openssl req -new -nodes -key certs/client/client.key -passin pass:p4ssphrase -out certs/client/client.csr -subj "/CN=postgres"
openssl x509 -req -in certs/client/client.csr -days 365 -CA certs/ca.crt -CAkey certs/ca.key -CAcreateserial -out certs/client/client.crt

SSL_RESOURCES="plugin-jdbc-postgres/src/test/resources/ssl"
mkdir -p "$SSL_RESOURCES"

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

docker compose -f docker-compose-ci.yml up --quiet-pull -d mariadb sqlserver postgres mysql clickhouse druid_postgres druid_zookeeper druid_coordinator druid_broker druid_historical druid_middlemanager druid_router oracle pinot
docker compose -f docker-compose-ci.yml up --quiet-pull -d --wait
sleep 3

docker exec -i plugin-jdbc-mariadb-1 mariadb -uroot -pmariadb_passwd --database=kestra -e """
INSTALL SONAME 'auth_ed25519';
CREATE USER 'ed25519'@'%' IDENTIFIED VIA ed25519 USING PASSWORD('secret');
GRANT SELECT ON kestra.* TO 'ed25519'@'%' IDENTIFIED VIA ed25519 USING PASSWORD('secret');
"""

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
      docker compose -f docker-compose-ci.yml logs --tail=200 "$name" || true
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

wait_for "postgres-multi-query" "docker compose -f docker-compose-ci.yml exec -T postgres-multi-query pg_isready -U postgres -d kestra" 600
wait_for "mysql" "docker compose -f docker-compose-ci.yml exec -T mysql mysqladmin ping -h127.0.0.1 -uroot -pmysql_passwd" 600
wait_for "clickhouse" "curl -sf http://127.0.0.1:28123/ping | grep -q Ok" 600
wait_for "druid_coordinator" "curl -sf http://127.0.0.1:11081/status/health | grep -q true" 600 5
wait_for "druid_broker"      "curl -sf http://127.0.0.1:11082/status/health | grep -q true" 600 5
wait_for "druid_historical"  "curl -sf http://127.0.0.1:11083/status/health | grep -q true" 600 5
wait_for "druid_middlemanager" "curl -sf http://127.0.0.1:11091/status/health | grep -q true" 600 5
wait_for "druid_router"      "curl -sf http://127.0.0.1:8888/status/health | grep -q true" 600 5
wait_for "oracle" "docker compose -f docker-compose-ci.yml exec -T oracle bash -lc 'export ORACLE_HOME=/u01/app/oracle/product/11.2.0/xe; export PATH=\$ORACLE_HOME/bin:\$PATH; export ORACLE_SID=XE; echo \"select 1 from dual;\" | sqlplus -s system/oracle >/dev/null'" 900 5
wait_for_tcp "pinot" 127.0.0.1 49000 420

echo "Configuring Oracle..."
sleep 20
docker exec plugin-jdbc-oracle-1 /bin/bash -c "export ORACLE_HOME=/u01/app/oracle/product/11.2.0/xe && export PATH=\$ORACLE_HOME/bin:\$PATH && export ORACLE_SID=XE && echo 'ALTER SYSTEM SET processes=500 SCOPE=SPFILE;' | sqlplus -s system/oracle"
docker exec plugin-jdbc-oracle-1 /bin/bash -c "export ORACLE_HOME=/u01/app/oracle/product/11.2.0/xe && export PATH=\$ORACLE_HOME/bin:\$PATH && export ORACLE_SID=XE && echo 'ALTER SYSTEM SET sessions=555 SCOPE=SPFILE;' | sqlplus -s system/oracle"
docker compose -f docker-compose-ci.yml restart oracle