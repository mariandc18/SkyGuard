COMPOSE_FILE=docker-compose.yaml
PROJECT_NAME=hadoop_cluster

run:
	docker-compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) up -d

down:
	docker-compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) down

restart:
	docker-compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) down && docker-compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) up -d

logs:
	docker-compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) logs -f

ps:
	docker-compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) ps

namenode:
	docker exec -it namenode bash

datanode:
	docker exec -it datanode bash

zookeeper:
	docker exec -it zookeeper bash

kafka:
	docker exec -it kafka-new bash

clean:
	docker-compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) down -v
	docker rm -f zookeeper bash
	docker rm -f kafka-new bash
	docker rm -f namenode bash
	docker rm -f datanode bash
producer:
	python scripts/producer.py --string HELLO --topic light_bulb
consumer:
	python scripts/consumer_elastic.py
model_api:
    python3 -m uvicorn scripts/api:app --host 0.0.0.0 --port 8000 --reload
