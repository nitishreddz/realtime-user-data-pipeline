# Real-Time User Data Pipeline

A production-ready data engineering project that demonstrates end-to-end real-time data streaming and processing. This pipeline fetches user data from an external API (i.e https://randomuser.me/api/), processes it through Kafka and Spark Streaming, and stores it in a Cassandra database.

## Architecture Overview

The pipeline implements a modern data architecture using the following components:

- **Apache Airflow**: Orchestrates the data ingestion workflow and schedules periodic data fetching
- **Apache Kafka**: Acts as the distributed message broker for real-time data streaming
- **Apache Spark**: Processes streaming data with structured streaming capabilities
- **Apache Cassandra**: Serves as the NoSQL database for storing processed user records
- **Docker**: Containerizes all services for easy deployment and scalability

## Project Structure

```
.
├── dags/
│   └── kafka_stream.py          # Airflow DAG for data ingestion
├── script/
│   └── entrypoint.sh             # Initialization script for Airflow
├── docker-compose.yaml           # Docker orchestration configuration
├── requirements.txt              # Python dependencies
└── spark_stream.py               # Spark streaming application
```

## Data Flow

1. Airflow DAG triggers a scheduled task that fetches random user data from the Random User API
2. The fetched data is formatted and published to a Kafka topic named 'users_created'
3. Spark Streaming consumes messages from the Kafka topic in real-time
4. Data is transformed and enriched with unique identifiers
5. Processed records are written to Cassandra database for persistence

## Prerequisites

Before running this project, ensure you have the following installed:

- Docker (version 20.10 or higher)
- Docker Compose (version 2.0 or higher)
- Python 3.9 or higher
- At least 8GB of available RAM for running all containers

## Installation

Clone the repository to your local machine:

```bash
git clone https://github.com/nitishreddz/realtime-user-data-pipeline.git
cd realtime-user-data-pipeline
```

## Running the Pipeline

Start all services using Docker Compose:

```bash
docker-compose up -d
```

This command will spin up the following services:

- Zookeeper (port 2181)
- Kafka Broker (port 9092)
- Airflow Webserver (port 8080)
- Airflow Scheduler
- PostgreSQL (for Airflow metadata)
- Spark Master (port 9090)
- Spark Worker
- Cassandra (port 9042)

## Accessing the Services

Once all containers are running, you can access:

- **Airflow UI**: http://localhost:8080
  - Default credentials: airflow/airflow
  - Navigate to the DAGs page and enable the 'user_automation' DAG
  
- **Spark Master UI**: http://localhost:9090
  - Monitor Spark jobs and worker nodes

## Running the Spark Streaming Job

After the containers are up and data is flowing through Kafka, execute the Spark streaming application:

```bash
python spark_stream.py
```

The application will connect to Kafka, consume messages, and write processed data to Cassandra.

## Database Schema

The Cassandra database contains a keyspace named 'spark_streams' with the following table structure:

**Table**: created_users

| Column | Type | Description |
|--------|------|-------------|
| id | UUID | Primary key, auto-generated |
| first_name | TEXT | User's first name |
| last_name | TEXT | User's last name |
| gender | TEXT | User's gender |
| address | TEXT | Full formatted address |
| post_code | TEXT | Postal code |
| email | TEXT | Email address |
| username | TEXT | Username |
| registered_date | TEXT | Registration timestamp |
| phone | TEXT | Phone number |
| picture | TEXT | Profile picture URL |

## Configuration

### Kafka Configuration

The Kafka broker is configured with the following settings:

- Bootstrap servers: localhost:9092 (external), broker:29092 (internal)
- Topic: users_created
- Replication factor: 1

### Spark Configuration

The Spark application uses:

- Cassandra Connector: 3.5.1
- Kafka Connector: 3.5.3
- Connection to Cassandra on localhost:9042

### Airflow Configuration

The DAG is scheduled to run daily and will stream data for 3 minutes per execution.

## Optional Components for Enhanced Monitoring

The docker-compose.yaml file includes commented-out configurations for two additional Kafka components that can be enabled for enhanced monitoring and management:

### Schema Registry

The Confluent Schema Registry provides a centralized repository for managing and validating schemas for Kafka messages. When enabled, it runs on port 8081 and helps ensure data compatibility across producers and consumers. This is particularly useful for enforcing data contracts and managing schema evolution over time.

### Control Center

Confluent Control Center is a web-based management and monitoring tool for Kafka clusters. When enabled, it provides a comprehensive UI accessible at http://localhost:9021 where you can:

- Monitor Kafka topics, partitions, and consumer groups in real-time
- Visualize message throughput and latency metrics
- Inspect messages flowing through Kafka topics
- Manage topic configurations and consumer offsets
- Track end-to-end stream processing performance

To enable these components, simply uncomment the relevant sections in the docker-compose.yaml file and restart the services. Note that enabling Control Center will require additional system resources (approximately 2-3GB of RAM).

## Monitoring and Troubleshooting

Check the status of all containers:

```bash
docker-compose ps
```

View logs for a specific service:

```bash
docker-compose logs -f [service_name]
```

For example, to view Kafka logs:

```bash
docker-compose logs -f broker
```

## Stopping the Pipeline

To stop all services:

```bash
docker-compose down
```

To stop and remove all data volumes:

```bash
docker-compose down -v
```

## Technology Stack

- Python 3.9
- Apache Airflow 2.6.0
- Apache Kafka 7.4.0
- Apache Spark 3.5.3
- Apache Cassandra (latest)
- PostgreSQL 14.0
- Docker and Docker Compose

## Future Enhancements

- Add data validation and quality checks
- Implement error handling and retry mechanisms
- Add monitoring with Prometheus and Grafana
- Implement data partitioning strategies for better performance
- Add unit and integration tests
- Configure security and authentication for production use
- Implement data backup and recovery procedures

## Contributing

Contributions are welcome. Please feel free to submit a Pull Request.

## License

This project is open source and available for educational purposes.

## Acknowledgments

- Random User API for providing test data
- Apache Software Foundation for the amazing open-source tools
- The data engineering community for inspiration and best practices
