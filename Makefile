.PHONY: up simulate down clean logs ps psql topics kafka-ui lint

up:            ## Start the pipeline with live Coinbase data
	docker compose up -d --build

simulate:      ## Start the pipeline with the offline trade simulator
	SOURCE=simulate docker compose up -d --build

down:          ## Stop all services (keeps data)
	docker compose down

clean:         ## Stop everything and delete all data
	docker compose down -v

logs:          ## Follow producer and Spark logs
	docker compose logs -f producer spark-streaming

ps:
	docker compose ps

psql:          ## Open a SQL shell on TimescaleDB
	docker compose exec timescaledb psql -U market -d market

topics:        ## Describe Kafka topics
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --describe

lint:
	ruff check . && ruff format --check .
