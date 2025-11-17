# This file has to be executed from the root of the project.
# Its purpose is to setup the test environment for the unit tests
# Especially, it generates certificates and stores them in the `plugin-jdbc-postgres/src/test/resources/ssl/` folder.
#
# It requires Docker and Docker Compose to be installed.
#
# Run it with the following command:
# sudo ./local-setup-unit.sh

echo "Cleaning up artifacts from previous runs..."
rm -rf certs openssl.cnf
rm -rf plugin-jdbc-postgres/src/test/resources/ssl/
mkdir -p certs/server certs/client plugin-jdbc-postgres/src/test/resources/ssl/

echo "Creating OpenSSL configuration file with required extensions..."
cat > openssl.cnf <<- "EOF"
[ req ]
distinguished_name = req_distinguished_name
prompt = no

[ req_distinguished_name ]
# This section is intentionally left blank.
# The subject will be provided via the -subj command-line argument.

# Extensions for the Root Certificate Authority
[ v3_ca ]
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always,issuer
basicConstraints = critical, CA:true
keyUsage = critical, digitalSignature, cRLSign, keyCertSign

# Extensions for the Server Certificate
[ v3_server ]
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always,issuer
basicConstraints = critical, CA:FALSE
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
EOF

echo "Generating Root CA..."
openssl genrsa -out certs/ca.key 2048
openssl req -x509 -new -nodes -key certs/ca.key -sha256 -days 365 -subj "/CN=root-ca" -config openssl.cnf -extensions v3_ca -out certs/ca.crt

echo "Generating Server Certificate..."
openssl genrsa -out certs/server/server.key 2048
openssl req -new -key certs/server/server.key -subj "/CN=postgresql" -out certs/server/server.csr
openssl x509 -req -in certs/server/server.csr -CA certs/ca.crt -CAkey certs/ca.key -CAcreateserial -sha256 -days 365 -extfile openssl.cnf -extensions v3_server -out certs/server/server.crt
sudo chmod 600 certs/server/server.key
sudo chown 1001 certs/server/server.key

echo "Generating Client Certificate (in PKCS#8 format)..."
openssl genrsa -out certs/client/client.key 2048
openssl req -new -key certs/client/client.key -subj "/CN=postgres" -out certs/client/client.csr
openssl x509 -req -in certs/client/client.csr -CA certs/ca.crt -CAkey certs/ca.key -CAcreateserial -sha256 -days 365 -out certs/client/client.crt
openssl pkcs8 -topk8 -inform PEM -in certs/client/client.key -out certs/client/client-no-pass.key -nocrypt

echo "Verifying server certificate against the CA..."
openssl verify -CAfile certs/ca.crt certs/server/server.crt

echo "Copying certificates to test resources..."
cp certs/client/* plugin-jdbc-postgres/src/test/resources/ssl/
cp certs/ca.crt plugin-jdbc-postgres/src/test/resources/ssl/
sudo chown -R "$USER":staff plugin-jdbc-postgres/src/test/resources/ssl
sudo chmod -R 755 plugin-jdbc-postgres/src/test/resources/ssl


docker compose -f docker-compose-ci.yml down -v
docker compose -f docker-compose-ci.yml up --quiet-pull -d --wait
docker compose -f docker-compose-ci.yml up --quiet-pull -d mariadb sqlserver
sleep 3

docker exec -i plugin-jdbc-mariadb-1 mariadb -uroot -pmariadb_passwd --database=kestra -e """
INSTALL SONAME 'auth_ed25519';
CREATE USER 'ed25519'@'%' IDENTIFIED VIA ed25519 USING PASSWORD('secret');
GRANT SELECT ON kestra.* TO 'ed25519'@'%' IDENTIFIED VIA ed25519 USING PASSWORD('secret');
"""
