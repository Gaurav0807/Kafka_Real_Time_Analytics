# Kafka Producer and Consumer with PostgreSQL Loader

## Overview

This repository contains scripts for setting up a basic Apache Kafka environment using Docker, implementing a producer script, a consumer script, and loading data into PostgreSQL.

## Prerequisites

- Docker installed on your machine.
- Python 3.8 installed.
- PostgreSQL database set up.

## Kafka Setup

1. Run Docker Compose to start Kafka and Zookeeper containers.

    bash
    docker-compose up -d
    

2. Check if Kafka and Zookeeper containers are running.

    bash
    docker ps
    

## Kafka Producer

### Running the Producer

1. Navigate to the producer directory.

    bash
    cd producer
    

2. Install Python dependencies.

    bash
    pip install -r requirements.txt
    

3. Run the Kafka producer script.

    bash
    python3 producer.py
    

### Producer Configuration

- Edit producer.py to modify the data being produced and Kafka topic details.

## Kafka Consumer

### Running the Consumer

1. Navigate to the consumer directory.

    bash
    cd real_time_api
    

2. Install Python dependencies.

    bash
    pip install -r requirements.txt
    

3. Run the Kafka consumer script to load data into PostgreSQL.

    bash
    python3 consumer.py
    

### Consumer Configuration

- Edit consumer.py to customize the consumer behavior, Kafka topic, and other settings.
- Ensure PostgreSQL connection details are correctly configured in the consumer script.

## Loading Data into PostgreSQL

1. Ensure your PostgreSQL database is running.

2. Modify the consumer script (consumer.py) to include logic for inserting or updating data into your PostgreSQL database. Use a PostgreSQL library like psycopg2 to interact with the database.

    Edit consumer.py(Script) to adjust database connection details and data loading behavior.

## Additional Notes

    - Modify Docker Compose file (docker-compose.yml) for advanced Kafka configurations.
    - Ensure proper network configurations between containers.

