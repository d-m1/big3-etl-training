#!/usr/bin/env bash
set -e

if [ "$1" = "scheduler" ]; then
    airflow db init
    airflow users create -r Admin -u admin -e admin@admin.com -f admin -l admin -p admin || true
    exec airflow scheduler
elif [ "$1" = "webserver" ]; then
    sleep 30
    exec airflow webserver
else
    exec "$@"
fi