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

### Druid test setup
echo "waiting for Druid to be ready..."
until curl -sf http://localhost:8888/status >/dev/null 2>&1; do
  echo "Waiting for druid-router..."
  sleep 3
done

until curl -sf http://localhost:11081/status >/dev/null 2>&1; do
  echo "waiting for druid-coordinator..."
  sleep 3
done

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