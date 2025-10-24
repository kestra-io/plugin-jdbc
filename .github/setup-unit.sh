#!/bin/bash
set -e

echo "====== STARTING DRUID SETUP ======"
echo "Time: $(date)"
echo ""

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

echo ""
echo "====== CHECKING EXISTING DOCKER STATE ======"
echo "Existing containers:"
docker ps -a | grep -E "(druid|postgres|mariadb|sqlserver)" || echo "None"
echo ""
echo "Existing volumes:"
docker volume ls | grep -E "(metadata|druid)" || echo "None"
echo ""

echo "====== STARTING DATABASES ======"
echo "Starting: mariadb, sqlserver, postgres"
docker compose -f docker-compose-ci.yml up --quiet-pull -d mariadb sqlserver postgres

echo ""
echo "====== STARTING ALL DRUID SERVICES ======"
echo "Starting all services with --wait..."
docker compose -f docker-compose-ci.yml up --quiet-pull -d --wait

echo ""
echo "====== CHECKING CONTAINER STATUS AFTER STARTUP ======"
docker ps -a | grep -E "(druid|postgres)" | head -20
echo ""

echo "====== CHECKING druid_postgres SPECIFICALLY ======"
if docker ps -a | grep druid_postgres | grep -q "Up"; then
  echo "✓ druid_postgres is running"
elif docker ps -a | grep druid_postgres | grep -q "Exited"; then
  echo "✗ druid_postgres EXITED!"
  echo ""
  echo "Full logs from druid_postgres:"
  docker logs druid_postgres
  echo ""
  echo "Container inspect:"
  docker inspect druid_postgres --format='Status: {{.State.Status}}, ExitCode: {{.State.ExitCode}}, Error: {{.State.Error}}'
  exit 1
else
  echo "✗ druid_postgres NOT FOUND!"
  exit 1
fi

sleep 3

docker exec -i plugin-jdbc-mariadb-1 mariadb -uroot -pmariadb_passwd --database=kestra -e """
INSTALL SONAME 'auth_ed25519';
CREATE USER 'ed25519'@'%' IDENTIFIED VIA ed25519 USING PASSWORD('secret');
GRANT SELECT ON kestra.* TO 'ed25519'@'%' IDENTIFIED VIA ed25519 USING PASSWORD('secret');
"""

echo ""
echo "====== WAITING FOR DRUID SERVICES ======"
echo "Waiting for druid-router..."
attempt=0
until curl -sf http://localhost:8888/status >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  if [ $attempt -gt 60 ]; then
    echo "Router timeout!"
    docker logs druid_router --tail 30
    exit 1
  fi
  echo "Waiting for druid-router... ($attempt)"
  sleep 3
done
echo "✓ Router is up!"

echo ""
echo "Waiting for druid-coordinator..."
attempt=0
max_attempts=60

until curl -sf http://localhost:11081/status >/dev/null 2>&1; do
  attempt=$((attempt + 1))

  # Check if druid_postgres is still running every 10 attempts
  if [ $((attempt % 10)) -eq 0 ]; then
    echo ""
    echo "--- Status check at attempt $attempt ---"
    if docker ps | grep -q druid_postgres; then
      echo "✓ druid_postgres is still running"
    else
      echo "✗ druid_postgres STOPPED RUNNING!"
      docker ps -a | grep druid_postgres
      echo ""
      echo "druid_postgres logs:"
      docker logs druid_postgres --tail 50
      exit 1
    fi

    if docker ps | grep -q druid_coordinator; then
      echo "✓ druid_coordinator is running"
      echo "Last 10 lines of coordinator logs:"
      docker logs druid_coordinator --tail 10
    else
      echo "✗ druid_coordinator stopped!"
      docker logs druid_coordinator --tail 50
      exit 1
    fi
    echo "---"
    echo ""
  fi

  if [ $attempt -ge $max_attempts ]; then
    echo ""
    echo "====== TIMEOUT - FULL DIAGNOSTIC ======"
    echo ""
    echo "All containers:"
    docker ps -a
    echo ""
    echo "Coordinator logs (last 100 lines):"
    docker logs druid_coordinator --tail 100
    echo ""
    echo "Postgres logs (last 50 lines):"
    docker logs druid_postgres --tail 50
    echo ""
    echo "ZooKeeper logs (last 30 lines):"
    docker logs druid_zookeeper --tail 30
    exit 1
  fi

  echo "waiting for druid-coordinator... ($attempt/$max_attempts)"
  sleep 5
done

echo "✓ Coordinator is up!"
echo ""

# preloading druid datasource
echo "====== LOADING TEST DATASOURCE ======"
curl -s -X POST http://localhost:8888/druid/v2/sql/statements \
  -H "Content-Type: application/json" \
  -d '{
    "query": "REPLACE INTO products OVERWRITE ALL WITH ext AS (
      SELECT * FROM TABLE(EXTERN(
        '\''{\"type\":\"inline\",\"data\":\"Index,Name\\n1,John\\n2,Alice\\n3,Bob\\n4,Carol\\n5,David\"}'\'',
        '\''{\"type\":\"csv\",\"findColumnsFromHeader\":true}\"'\''
      )) EXTEND (\"Index\" BIGINT, \"Name\" VARCHAR)
    )
    SELECT TIME_PARSE('\''2000-01-01 00:00:00'\'') AS __time, * FROM ext PARTITIONED BY ALL",
    "context": {"executionMode": "ASYNC", "maxNumTasks": 2}
  }' >/dev/null

echo "Waiting for 'products' datasource..."
until curl -sf http://localhost:11081/druid/coordinator/v1/datasources | grep -q products; do
  sleep 2
done

echo "✓ Datasource loaded!"
echo ""
echo "====== DRUID SETUP COMPLETE ======"