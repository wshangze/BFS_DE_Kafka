# Project Overview
Let's say now you are working at a finance company, and your manager wants you to build a Kafka-based monitoring system for tracking currency changes. It integrates with CoinCap to retrieve live currency data, allowing users to create different consumer groups and keys to monitor specific currencies (e.g., BTC, ETH) as needed. The setup includes essential components like Kafka, Zookeeper, PostgreSQL, and custom Python APIs for data retrieval and consumer group management. \
\
As of now, the system only monitors the BTC product and simply dumps all the data into a Postgre db table, but you can add as many services as you like to implement some trading strategy (moving average crossover strat for instance).

# Components

## Zookeeper
Zookeeper is a distributed coordination service that Kafka uses to manage brokers and metadata. It listens on port `2181` and is essential for managing Kafka brokers within the cluster.

## Kafka Broker
The Kafka broker is the main message processing service that allows the producer to send data and consumers to retrieve data. It connects to Zookeeper and exposes a listener at port `29092` for local access.

## Database (PostgreSQL)
A PostgreSQL database is included to store relevant data. It runs on port `5432` and is configured to persist data using a Docker volume for durability across container restarts.

## Producer API
The producer service is a Python script (`api.py`) that fetches data from the CoinCap API and pushes it to the Kafka topic. It serves as the data ingestion point for the currency tracking system. The producer API is accessible on port `5000`.

## Trade Consumer
The trade consumer (`TradeConsumer.py`) is a consumer service that listens for currency changes based on trade-related consumer groups, such as `trade group`. It processes data related to specific currency keys, like BTC or ETH, and stores relevant information in the PostgreSQL database. The trade consumer service runs on port `6000`.

## DA Consumer (Optional)
An additional consumer service (`DAConsumer.py`) is configured but commented out in the Docker Compose file. This service is for a `DA group`, another consumer group that could be set up to listen for similar currency changes or other data streams if needed. It runs on port `6500` when enabled.

# Usage
1. **Start the Project**: Run the Docker Compose file to start all services.
   ```bash
   docker-compose build
   docker-compose up -d

2. **Connect to a database and create a table**: Connect to your postgre db on port 5432. Note that I choose team1 (trade team) as a schema name. You can include it as a table name, or whatever
   ```bash
   create table team1.BTC (retrieve_time timestamp, price_usd varchar(50), change_percent varchar(50));
2. **Run some SQL script**:  you should see some results already
   ```bash
   SELECT * FROM team1.BTC;

# Optional Exercise
1. Modify the docker-compose.yml file so that there are 2 consumers for the trade team and 2 consumers for the DA team.
2. Think about how do you manage messages for different consumer groups for different products (what should be the topic? what should be the key?) There is no right or wrong answer to it.
3. Add another producer for some other product. 
4. Add logic for the DA consumer group (DAconsumer.py). 
5. Play around with the topic, key, partitions, offset etc now that you have multiple consumers and multiple topics.
   


