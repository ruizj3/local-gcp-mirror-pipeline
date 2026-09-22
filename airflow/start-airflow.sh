#!/usr/bin/env bash
set -e

airflow db init
airflow users create \
  --username admin \
  --password admin \
  --firstname First \
  --lastname Last \
  --role Admin \
  --email admin@example.com || true
exec airflow webserver
