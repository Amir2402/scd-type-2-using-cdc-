up:
	sudo docker-compose up -d

down:
	sudo docker-compose down

list-topics:
	sudo docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

connect-debezium:
	curl http://localhost:8083/connectors \
    -H 'Content-Type: application/json' \
    -X POST \
    -d '@docker/debezium/debezium-connector-config.json'

connector-status:
	curl -H "Accept:application/json" localhost:8083/connectors/postgresql-connector/status