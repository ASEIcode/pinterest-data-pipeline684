# Pinterest Data Pipeline Project

## Table of Contents
1. [Overview](#overview)
2. [Features](#features)
3. [Architecture](#architecture)
4. [Getting Started](#getting-started)
    - [Prerequisites](#prerequisites)
    - [Installation](#installation)
5. [Usage](#usage)
    - [Batch Processing](#batch-processing)
    - [Stream Processing](#stream-processing)
    - [Data Exploration](#data-exploration)
    - [Scheduling and Automation](#scheduling-and-automation)
6. [Configuration](#configuration)
7. [Troubleshooting](#troubleshooting)
8. [Advanced Features](#advanced-features)
9. [Contributing](#contributing)
10. [License](#license)
11. [Acknowledgments](#acknowledgments)

## Overview

This pipeline is engineered to streamline the processing and analysis of large datasets, drawing inspiration from Pinterest's infrastructure. It orchestrates the seamless integration of data ingestion, storage, and real-time analysis, leveraging a robust suite of technologies. By unifying AWS services, Apache Kafka, Databricks, and Apache Airflow, it exemplifies a sophisticated approach to data engineering, designed to accommodate both the scalability demands and the analytical depth required by modern data ecosystems.

## Features

- **Data Ingestion and Storage**
  - Utilizes **AWS S3** for secure, scalable storage of batch data.
  - Integrates **AWS API Gateway** and **Kafka** (via AWS MSK) for efficient real-time data streaming.

- **Data Processing**
  - Leverages **Databricks** for advanced data analytics, employing **Apache Spark** for both batch and stream processing.
  - Implements **Lambda architecture** for handling vast datasets with a balance of speed and accuracy.

- **Workflow Orchestration**
  - Uses **Apache Airflow** (AWS MWAA) for orchestrating and automating the data pipeline workflows, ensuring timely execution of data processing tasks.

- **Real-Time Data Streaming**
  - Incorporates **AWS Kinesis** for real-time data collection and analysis, enabling immediate insights and responses.

- **Data Cleaning and Transformation**
  - Applies comprehensive data cleaning techniques in **Databricks notebooks** to ensure data quality and reliability for analysis.
  - Employs custom **Python** scripts and **Spark SQL** for data transformation and preparation.

- **Analytics and Reporting**
  - Facilitates advanced data analytics within Databricks, using both **PySpark** and **Spark SQL** for deep dives into the data.

- **Security and Compliance**
  - Adheres to best practices in cloud security, leveraging AWS's robust security features to protect data integrity and privacy.

- **Scalability and Performance**
  - Designed for scalability, easily handling increases in data volume without compromising on processing time or resource efficiency.


## Architecture

# Pinterest Data Pipeline Project

## Table of Contents
1. [Overview](#overview)
2. [Features](#features)
3. [Architecture](#architecture)
4. [Getting Started](#getting-started)
    - [Prerequisites](#prerequisites)
    - [Installation](#installation)
5. [Usage](#usage)
    - [Batch Processing](#batch-processing)
    - [Stream Processing](#stream-processing)
    - [Data Exploration](#data-exploration)
    - [Scheduling and Automation](#scheduling-and-automation)
6. [Configuration](#configuration)
7. [Troubleshooting](#troubleshooting)
8. [Advanced Features](#advanced-features)
9. [Contributing](#contributing)
10. [License](#license)
11. [Acknowledgments](#acknowledgments)

## Overview

This pipeline is engineered to streamline the processing and analysis of large datasets, drawing inspiration from Pinterest's infrastructure. It orchestrates the seamless integration of data ingestion, storage, and real-time analysis, leveraging a robust suite of technologies. By unifying AWS services, Apache Kafka, Databricks, and Apache Airflow, it exemplifies a sophisticated approach to data engineering, designed to accommodate both the scalability demands and the analytical depth required by modern data ecosystems.

## Features

- **Data Ingestion and Storage**
  - Utilizes **AWS S3** for secure, scalable storage of batch data.
  - Integrates **AWS API Gateway** and **Kafka** (via AWS MSK) for efficient real-time data streaming.

- **Data Processing**
  - Leverages **Databricks** for advanced data analytics, employing **Apache Spark** for both batch and stream processing.
  - Implements **Lambda architecture** for handling vast datasets with a balance of speed and accuracy.

- **Workflow Orchestration**
  - Uses **Apache Airflow** (AWS MWAA) for orchestrating and automating the data pipeline workflows, ensuring timely execution of data processing tasks.

- **Real-Time Data Streaming**
  - Incorporates **AWS Kinesis** for real-time data collection and analysis, enabling immediate insights and responses.

- **Data Cleaning and Transformation**
  - Applies comprehensive data cleaning techniques in **Databricks notebooks** to ensure data quality and reliability for analysis.
  - Employs custom **Python** scripts and **Spark SQL** for data transformation and preparation.

- **Analytics and Reporting**
  - Facilitates advanced data analytics within Databricks, using both **PySpark** and **Spark SQL** for deep dives into the data.

- **Security and Compliance**
  - Adheres to best practices in cloud security, leveraging AWS's robust security features to protect data integrity and privacy.

- **Scalability and Performance**
  - Designed for scalability, easily handling increases in data volume without compromising on processing time or resource efficiency.


## Architecture

### Batch processing path

1. **Data Generation with User Posting Emulator:** The journey begins with the user posting emulator, which simulates user activities such as creating posts, comments, or likes. This tool generates synthetic data mimicking real user interactions on the platform.

2. **Data Ingestion via API Gateway:** The generated data from the emulator is then sent to AWS API Gateway, acting as the entry point for data into the AWS ecosystem. API Gateway efficiently manages these incoming data requests and routes them to the appropriate services for processing.

3. **Data Routing to EC2 Instance:** Once the data passes through the API Gateway, it's forwarded to an Amazon EC2 (Elastic Compute Cloud) instance. Here, preliminary processing or transformation occurs, such as data validation, formatting, or enrichment, to prepare the data for further processing stages.

4. **Streaming Data with Kafka and MSK:** For the data to be ingested into the streaming platform, it's published to an Apache Kafka topic within Amazon MSK (Managed Streaming for Apache Kafka). MSK provides a fully managed Kafka service, making it easier to build and run applications that process streaming data.

5. **Confluent Connect and S3 Kafka REST Proxy:** Confluent Connect, part of the Confluent Platform, facilitates the movement of data between Kafka and other systems like AWS S3. In this setup, a Kafka Connect S3 Sink connector is used to efficiently store the incoming streaming data into an S3 bucket for durable storage. Additionally, the S3 Kafka REST Proxy allows applications to produce and consume messages over HTTP, providing a bridge between Kafka topics and HTTP-based applications or services.

6. **Kafka Consumers with MSK and MSK Connect:** Kafka Consumers subscribe to specific topics within MSK to process or analyze the streaming data. MSK Connect, a feature of Amazon MSK, simplifies the deployment and management of connectors, enabling seamless data integration and processing. These consumers can be applications running on EC2 instances or serverless functions in AWS Lambda, which process the data further or move it into analytical platforms like Databricks for deeper analysis.

7. **Data Processing in Databricks:** The data, now stored in AWS S3 and made accessible via Kafka topics, is ingested into Databricks. Here, using Apache Spark, the data undergoes extensive processing, analysis, and transformation. Databricks facilitates scalable batch processing of the data, allowing for complex analytics, machine learning model training, or aggregation tasks to be performed efficiently.

8. **Workflow Orchestration with Apache Airflow:** Apache Airflow manages the pipeline's workflows, scheduling jobs, and ensuring dependencies are met. It orchestrates the entire batch processing workflow, from data ingestion to processing in Databricks, ensuring that each step is executed in the correct order and at the right time. There is one example DAG included with this project which shows how data could be ingested and cleaned ready for analysis on a daily basis. 

### Real-Time Data Streaming Path

1. **Real-Time Data Generation**
    - Just like the batch process, we start with the user posting emulator generating real-time events (e.g., posts, comments, likes), simulating user interactions.

2. **Data Ingestion via AWS Kinesis**
    - Real-time generated data is ingested into AWS Kinesis Data Streams, handling large volumes of real-time data for immediate capture and processing.

3. **Processing with Databricks and Apache Spark**
    - Data from Kinesis streams is processed by Databricks using Apache Spark's stream-processing features. Spark Streaming enables real-time data analysis, supporting operations like aggregations and windowing.

5. **Real-Time Analytics and Dashboards**
    - The processed data supports real-time analytics, visualized through dashboards in tools like Amazon QuickSight or Databricks notebooks, offering insights into various metrics.

6. **Workflow Orchestration with Apache Airflow**
    - While real-time processing is ongoing, Apache Airflow manages scheduled tasks, such as model updates or daily batch aggregations, alongside real-time stream processing.

7. **Notification and Actions**
    - Actions like sending notifications or initiating workflows can be automated based on real-time analytics. AWS Lambda functions can respond to specific conditions detected in the stream.


## Getting Started

### Prerequisites

requirements.txt, other software like kafka, confluent, api etc make a list here

### Installation

make several link files here referenceing different parts of the setup.

- AWS API gateway
- kafka
- RESTful api config
- DAG (this is nto a fulllist. go through docs and find it all)

## Usage

brief blurb here about the directory structure and overview of how everything functions

### Batch Processing

How to initiate the batch processing side of the pipeline.

### Stream Processing

how to initiate the stream processing kinesis path

### Data Exploration

explanation of what these files illustrate

### Scheduling and Automation

configuring your DAG and using airflow

## Configuration

## Troubleshooting

## Advanced Features

## Contributing

## License

## Acknowledgments


## Getting Started

### Prerequisites

requirements.txt, other software like kafka, confluent, api etc make a list here

### Installation

make several link files here referenceing different parts of the setup.

- AWS API gateway
- kafka
- RESTful api config
- DAG (this is nto a fulllist. go through docs and find it all)

## Usage

brief blurb here about the directory structure and overview of how everything functions

### Batch Processing

How to initiate the batch processing side of the pipeline.

### Stream Processing

how to initiate the stream processing kinesis path

### Data Exploration

explanation of what these files illustrate

### Scheduling and Automation

configuring your DAG and using airflow

## Configuration

## Troubleshooting

## Advanced Features

## Contributing

## License

## Acknowledgments
