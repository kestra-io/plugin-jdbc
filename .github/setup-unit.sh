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

docker compose -f docker-compose-ci.yml up --quiet-pull -d mariadb sqlserver postgres
docker compose -f docker-compose-ci.yml up --quiet-pull -d --wait
sleep 3

docker exec -i plugin-jdbc-mariadb-1 mariadb -uroot -pmariadb_passwd --database=kestra -e """
INSTALL SONAME 'auth_ed25519';
CREATE USER 'ed25519'@'%' IDENTIFIED VIA ed25519 USING PASSWORD('secret');
GRANT SELECT ON kestra.* TO 'ed25519'@'%' IDENTIFIED VIA ed25519 USING PASSWORD('secret');
"""

echo "waiting for Druid to be ready..."
until curl -sf http://localhost:8888/status >/dev/null 2>&1; do
  echo "Waiting for druid-router..."
  sleep 3
done
echo "Router is up!"

echo "Waiting for druid-coordinator..."
attempt=0
max_attempts=40
until curl -sf http://localhost:11081/status >/dev/null 2>&1; do
  attempt=$((attempt + 1))

  if [ $attempt -ge $max_attempts ]; then
    echo ""
    echo "====== TIMEOUT - DIAGNOSTIC INFO ======"
    echo ""

    echo "1. All Druid Containers Status:"
    docker ps -a | grep druid
    echo ""

    echo "2. Coordinator Restart Count:"
    docker inspect druid_coordinator --format='RestartCount: {{.RestartCount}}'
    echo ""

    echo "3. Coordinator State:"
    docker inspect druid_coordinator --format='State: {{.State.Status}}, Running: {{.State.Running}}, ExitCode: {{.State.ExitCode}}'
    echo ""

    echo "4. Last 50 Lines of Coordinator Logs:"
    docker logs druid_coordinator --tail 50
    echo ""

    echo "5. Druid Postgres Status:"
    docker ps | grep druid_postgres
    echo ""

    echo "6. Can coordinator reach postgres?"
    docker exec druid_coordinator sh -c "pg_isready -h druid_postgres -p 5432 -U druid" 2>&1 || echo "pg_isready not available, trying nc..."
    docker exec druid_coordinator sh -c "nc -zv druid_postgres 5432" 2>&1 || echo "Network check failed"
    echo ""

    echo "7. ZooKeeper Status:"
    docker ps | grep druid_zookeeper
    echo ""

    echo "8. Postgres Logs (last 20 lines):"
    docker logs druid_postgres --tail 20
    echo ""

    echo "====== END DIAGNOSTIC INFO ======"
    exit 1
  fi

  if [ $((attempt % 5)) -eq 0 ]; then
    echo "waiting for druid-coordinator... (attempt $attempt/$max_attempts - $((attempt * 3)) seconds elapsed)"
  else
    echo "waiting for druid-coordinator..."
  fi
  sleep 3
done

echo "Coordinator is up!"

# preloading druid datasource
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

# wait for datasource to be available
echo "Waiting for 'products' datasource"
until curl -sf http://localhost:11081/druid/coordinator/v1/datasources | grep -q products; do
  sleep 2
done

echo "Druid setup complete!"