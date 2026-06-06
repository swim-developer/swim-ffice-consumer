#!/bin/bash
set -euo pipefail

CERTS_DIR="$(cd "$(dirname "$0")" && pwd)"
CA_PASS="changeit"

echo "=== Generating local CA ==="
mkcert -install
cp "$(mkcert -CAROOT)/rootCA.pem" "$CERTS_DIR/ca.crt"
cp "$(mkcert -CAROOT)/rootCA-key.pem" "$CERTS_DIR/ca.key"

echo "=== Generating broker certificate ==="
mkcert -cert-file "$CERTS_DIR/broker.crt" -key-file "$CERTS_DIR/broker.key" \
  ffice-consumer-validator-artemis localhost 127.0.0.1

echo "=== Generating validator certificate ==="
mkcert -cert-file "$CERTS_DIR/validator.crt" -key-file "$CERTS_DIR/validator.key" \
  ffice-consumer-validator localhost 127.0.0.1

echo "=== Creating PKCS12 keystores ==="
openssl pkcs12 -export -in "$CERTS_DIR/broker.crt" -inkey "$CERTS_DIR/broker.key" \
  -out "$CERTS_DIR/broker.p12" -name broker -password "pass:$CA_PASS"

keytool -importcert -noprompt -alias ca -file "$CERTS_DIR/ca.crt" \
  -keystore "$CERTS_DIR/ca-truststore.p12" -storetype PKCS12 -storepass "$CA_PASS"

echo "=== Creating JKS keystores for consumer ==="
keytool -importcert -noprompt -alias ca -file "$CERTS_DIR/ca.crt" \
  -keystore "$CERTS_DIR/truststore.jks" -storepass "$CA_PASS"

mkcert -cert-file "$CERTS_DIR/consumer.crt" -key-file "$CERTS_DIR/consumer.key" \
  ffice-consumer localhost 127.0.0.1

openssl pkcs12 -export -in "$CERTS_DIR/consumer.crt" -inkey "$CERTS_DIR/consumer.key" \
  -out "$CERTS_DIR/consumer.p12" -name consumer -password "pass:$CA_PASS"

keytool -importkeystore -noprompt \
  -srckeystore "$CERTS_DIR/consumer.p12" -srcstoretype PKCS12 -srcstorepass "$CA_PASS" \
  -destkeystore "$CERTS_DIR/keystore.jks" -deststorepass "$CA_PASS"

echo "=== Done. Generated files: ==="
ls -la "$CERTS_DIR"
