#!/bin/bash
set -e

if [ -f /certs/broker.p12 ]; then
  echo "TLS certificates found, starting with AMQPS support"
else
  echo "No TLS certificates found, starting with AMQP only"
fi

exec /opt/activemq-artemis/bin/artemis run
