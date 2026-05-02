up:
	bash ./scripts/setup_environment.sh

down:
	sudo docker-compose down

list-topics:
	sudo docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

consume-changes:
	sudo docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic pg-changes.public.customers

connect-debezium:
	curl http://localhost:8083/connectors \
    -H 'Content-Type: application/json' \
    -X POST \
    -d '@docker/debezium/debezium-connector-config.json'

connector-status:
	curl -H "Accept:application/json" --write-out '\n%{http_code}' --silent --output /dev/null localhost:8083/connectors/postgresql-connector/status

push:
	git push -u origin main

init-table:
	sudo docker exec -i postgres psql -U postgres -d db_local < ./scripts/init_table.sql