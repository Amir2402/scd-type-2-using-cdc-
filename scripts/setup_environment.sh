#!/usr/bin/bash

echo launching containers $'\n'
sudo docker compose up -d --build

response=$(make connector-status)
http_code=$(echo "$response" | tail -n1)

if [[ $http_code -ne 200 ]]; then
    echo "connecting debezium to postgres"
    make connect-debezium
    echo "creating and inserting base values"
    make init-table
else
    echo "debezium connector is set"
fi

