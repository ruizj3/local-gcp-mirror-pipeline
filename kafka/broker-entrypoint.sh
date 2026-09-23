#!/usr/bin/env bash
set -e

port="${PORT:-10000}"

# Render may retain environment variables from an earlier Blueprint revision.
unset KAFKA_PORT

export KAFKA_LISTENERS="PLAINTEXT://0.0.0.0:${port}"
export KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://${RENDER_KAFKA_HOST:-kafka}:${port}"
export KAFKA_LISTENER_SECURITY_PROTOCOL_MAP="PLAINTEXT:PLAINTEXT"
export KAFKA_INTER_BROKER_LISTENER_NAME="PLAINTEXT"

exec /etc/confluent/docker/run
