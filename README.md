# Exploring Kafka Flink

## Overview
This project involves a sophisticated data pipeline designed to handle, process, and analyze transactions from simulated customers to identify fraudulent activities and generate actionable insights. It integrates multiple technologies including Kafka, Flink, and PostgreSQL, with a focus on performance and scalability.

## System Architecture
This section breaks down the pipeline into its core components, explaining their roles and interactions within the system.
![Arch](/pipeline-architecture.png)

### Data Generation Module
- **Purpose**: Generates mock data for customers and transactions, simulating a real-world stream of data.
- **Tools Used**: Utilizes the `confluent_kafka` library and `faker` to produce realistic customer and transaction data in JSON format.
- **Concurrency**: Employs multi-threading to accelerate data generation, ensuring high throughput in data production to Kafka topics.

### Kafka Integration
- **Role**: Serves as the central event streaming platform for all generated events, including customer data and transactions.
- **Scalability**: Configured to efficiently handle high volumes of data while providing robust data integrity and availability.

### Flink Processing Engine
- **Data Streaming**: Utilizes `flink-connector-kafka` to ingest data from Kafka topics continuously.
- **Windowing Functions**: Implements a 10-second tumbling window to compute total spending per customer, providing near-real-time financial analytics.
- **Data Enrichment**: Enriches transaction streams with customer details (name, email, birthdate) absent from the initial transaction feed by merging data streams before pushing the enriched data back to Kafka.

### PostgreSQL Database
- **Data Storage**: Acts as the sink for all processed data, storing enriched transactions and customer information.
- **Analytics**: Positioned to support further analytical queries and reporting needs.

## Future Enhancements
- **Data Deduplication**: Introduce mechanisms to deduplicate transaction data to clean up the dataset, removing any fraudulent transactions.
- **Business Intelligence Integration**: Connect with a BI tool such as Tableau to visualize key metrics, such as monthly customer expenditure and identifying peak transaction periods.
- **Observability Suite**: Implement a comprehensive observability platform to monitor the pipeline's performance and health, including logging, metrics, and KPI tracking.
- **Compliance and Reporting**: Consider adding modules for compliance tracking and automated reporting to meet industry standards and regulatory requirements.

## Conclusion
The pipeline is designed with extensibility in mind, ready to integrate additional modules for advanced analytics and enhanced monitoring, supporting continuous improvement and operational excellence.