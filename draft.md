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

Before setting up the Pinterest Data Pipeline project, ensure you have the following prerequisites ready:

- **AWS Cloud Account**: An active AWS account is required to access AWS services like S3, Lambda, API Gateway, Kinesis, and Managed Streaming for Kafka (MSK). [Sign up here](https://aws.amazon.com/) if you don't have an account yet.
- **Databricks Workspace**: You'll need a Databricks account for running Spark jobs and processing data. Databricks integrates with AWS to leverage cloud storage and compute. [Start a Databricks trial](https://databricks.com/try-databricks).
- **Python 3.x**: Ensure Python 3.x is installed on your system, as it's required to run the emulator scripts and interact with AWS SDKs and other libraries. [Download Python](https://www.python.org/downloads/).
- **Required Python Libraries**: The project depends on several Python libraries listed in the `requirements.txt` file, including `requests`, `sqlalchemy`, and others.
- **Git**: Version control is managed via Git. [Install Git](https://git-scm.com/downloads) if it's not already set up on your system.
- **IDE/Code Editor**: Although optional, having an Integrated Development Environment (IDE) like VSCode, or a simple code editor will make managing and editing the project code easier.

**Additional Setup Instructions:**
- **Configure AWS CLI**: Install and configure the AWS CLI with your AWS account credentials to interact with AWS services through the command line. [AWS CLI configuration guide](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html).
- **Set Up SSH Key for GitHub (if applicable)**: If you plan to clone the project repository via SSH, ensure you have SSH keys set up and added to your GitHub account. [GitHub SSH key setup guide](https://docs.github.com/en/authentication/connecting-to-github-with-ssh).

After ensuring all prerequisites are in place, you can proceed to the installation and setup instructions detailed in the next section of this README.


### Installation

1. **Clone the Repository**
   
   Clone the project repository to your local machine or development environment using SSH:
   ```bash
   git clone git@github.com:ASEIcode/pinterest-data-pipeline684.git
   cd pinterest-data-pipeline684
  
2. **Set Up Your AWS Environment**

    Configure the necessary AWS services such as S3, Lambda, API Gateway, Kinesis, and MSK:

    - **Create an S3 bucket for data storage**. See https://docs.aws.amazon.com/AmazonS3/latest/userguide/create-bucket-overview.html for instructions.

    - **Establish an MSK cluster and set up an EC2 client machine for Kafka:**
        https://colab.research.google.com/drive/1gYFc5W_TILdgDMprHXgwmjrY_xTmwsGS#scrollTo=Xk8JoWRrzZeq
        - Create your topics:

          To create a topic, make sure you are inside your `<KAFKA_FOLDER>/bin` and then run the following command, replacing **BoostrapServerString** with the connection string you have previously saved, and `<topic_name>` with your desired topic name:

          `./kafka-topics.sh --bootstrap-server BootstrapServerString --command-config client.properties --create --topic <topic_name>` 

          This projects files use 3 topics (< userID >.pin, < userID >.geo & < userID >.user) which are being generated from 3 different RDS tables. You can replace these with your own tables and topics. One for each data source / table. Ensure if you do that you replace/alter the above topic names with the new ones wherever they appear in the code files.

    - **Create a custom Plugin with MSK connect:**
      
       https://colab.research.google.com/drive/1zDDX7S1X2FxQF6Fnw6mmRmriEnTkYw9_?usp=sharing

    - **Set up API Gateway for data ingestion.**

      - Set up a REST API with AWS API gateway: https://colab.research.google.com/drive/1epCnS6ltyPtciuLG4vgWjte7pxXp-_vV?usp=sharing
      - Integrate the REST API with Kafka: https://colab.research.google.com/drive/1Zb_BaI8Nv-pL2mvr8yE1Y3d-sf4AP5lg?usp=sharing
      - Modify the **user_posting_emulation.py file**:  
        This script is currently set up to pull random rows from three different RDS tables to emuilate a stream of user activity (posts, geo data, and user details).  
        If you are going to use a different data source, this inital section which connects to an RDS database will need comstomising to fit your own data sources.

        The below instructions will work if you are using RDS databases:

        1. Create a dbcreds.yaml file in the following format:

            ```HOST : <RDS HOST NAME>
              USER : <YOUR USER NAME>
              PASSWORD : :<RDS PASSWORD>
              DATABASE : <DATABASE NAME>
              PORT : 3306
        2. Add the dbcreds file to your .gitignore file, so that your database credentials remain secure. They will securely loaded using the following code in the emulation file:

            
                class AWSDBConnector:

                  def __init__(self):
                      pass

                  def read_db_creds(self):
                      """
                      Reads and Returns the database Credentials from the YAML file (not included in Git Repo).
                      """

                      with open("db_creds.yaml", "r") as f:
                          creds = yaml.safe_load(f)
                          return creds
                      
                      def create_db_connector(self):
                          creds = self.read_db_creds()
                          engine = sqlalchemy.create_engine(f"mysql+pymysql://{creds['USER']}:{creds['PASSWORD']}@{creds['HOST']}:{creds['PORT']}/{creds['DATABASE']}?charset=utf8mb4")
                          return engine
        3. In the run_infinite_post_data_loop() function, replace the table names with your own RDS table names: 

        E.g. 

            with engine.connect() as connection:

              <YOUR_TOPIC_NAME>_string = text(f"SELECT * FROM <YOUR_TABLE_NAME> LIMIT {random_row}, 1")
              pin_selected_row = connection.execute(pin_string)
              
              for row in pin_selected_row:
                  pin_result = dict(row._mapping)
          You nay also want to change the variable names (pin_string / pin_result / pin_payload etc) to match the names of each of your topics.

        4. Modify the column names and data types in each payload to match your data.
        5. Replace the invoke links in each of the response variables ():
        
                pin_response = requests.request("POST","<"https://YourAPIInvokeURL/YourDeploymentStage/topics/YourTopicName">/, headers=headers, data=pin_payload)
            Once again modify the variable names to match your topics. Dont forget to change the data=pin_payload to your own payload name.
        6. Make sure you Have completed steps 4 and 5 for each of the topics / RDS tables before saving and moving on.
        7. Check data is sent to the cluster by running a Kafka consumer (one per topic). If everything has been set up correctly, you should see messages being consumed.
        8. Check if data is getting stored in the S3 bucket. Notice the folder organization (e.g topics/<TOPIC_NAME>/partition=0/) that your connector creates in the bucket.

    - **Configure Kinesis Data Streams for real-time data processing.**  

        1. Create a data stream for each of your emulator file payloads (data sources) using **Kinesis Data streams**

            - Navigate to the **Kinesis console**, and select the **Data Streams** section. Choose the **Create stream button**.

            - Choose the desired name for your stream and input this in the Data stream name field. For our use case we will use the **On-demand capacity mode**.

            - Once you have entered the name and chose the capacity mode click on **Create data stream**. When your stream is finished creating the Status will change from Creating to Active.
     
        3. Configure your previously created REST API to allow it to invoke Kinesis actions.  

            You can find instructions for this here: https://colab.research.google.com/drive/1GnwFW22hNpslDmq6N73fXc7zjqhebXp-?usp=sharing
        4. Modify the user_posting_emulation_streaming.py script:

            - Follow the steps from earlier when we modified your user_posting_emulation.py script. The only difference is that this time the format of each payload has been changed to make it compatible with the kinesis stream. 
            - Replace **StreamName** with your own stream names
            - Replace **Data** with your own table's column names
            - Replace **PartitionKey** with your own chosen name
            - Finally each invoke link in each reponse variable needs to be replaced with:

                  "https://YourAPIInvokeURL/<YourDeploymentStage>/streams/<your_stream_name>/record"

3. **Configure Databricks Workspace**

    - Import the project's notebooks into your Databricks workspace. [Instructions](https://docs.databricks.com/en/notebooks/notebook-export-import.html)

    - Set up a cluster : [Instructions](https://colab.research.google.com/drive/1huJvijXGCqU3-lJCazj37Mdl9jzH_YTR?usp=sharing)
    - Mount your S3 storage bucket to databricks using the mount_s3_to_databricks.py notebook
      You will need to change the following code within this file to match theb location / format of your own credentials:
      ```
      # Define the path to the Delta table
      delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"

      # Read the Delta table to a Spark DataFrame
      aws_keys_df = spark.read.format("delta").load(delta_table_path)

4. **Initialize Apache Airflow Environmen (MWAA)**
    Set up your Apache Airflow environment using AWS Managed Workflows for Apache Airflow (MWAA), and configure the project's workflows as DAGs. One example DAG is included in the batch processing folder of this project.

    - Set up the initial MWAA environment : [Instructions](https://colab.research.google.com/drive/1m4PQq2xbfvOqJ_4xt34aRRRKW1nNsP_P?usp=sharing)
    - How to orchestrate a workload: [Instructions](https://colab.research.google.com/drive/1Zzwkce_sSr51cV5FyhtsCWhhSdj0XlL_?usp=sharing)

## Usage

This project is structured to facilitate both batch and real-time stream processing of data, simulating a comprehensive data pipeline akin to those used by large-scale social media platforms like Pinterest. Below is an overview of how to initiate and leverage both processing paths within this project.

### Batch Processing

The batch processing component is structured to manage large volumes of data efficiently, simulating a robust pipeline for data analysis, cleansing, and transformations.

**To initiate batch processing:**

1. **Log into the EC2 Client Machine**: SSH into your EC2 instance that hosts the REST API. This API serves as the intermediary, receiving data payloads from the User Posting Emulator and forwarding them for processing.

    ```bash
    ssh -i /path/to/your-key.pem ec2-user@your-ec2-public-dns.amazonaws.com
    ```
    
    Replace `/path/to/your-key.pem` and `ec2-user@your-ec2-public-dns.amazonaws.com` with your actual SSH key path and EC2 instance's DNS.

2. **Start the REST API**: In the EC2 instance, navigate to /home/ec2-user/confluent-7.2.0/bin and initiate the REST API server to listen for incoming requests from the AWS API Gateway.

    ```bash
    ./kafka-rest-start /home/ec2-user/confluent-7.2.0/etc/kafka-rest/kafka-rest.properties
    ```

3. **Run the User Posting Emulator**: With the REST API up and running, start the emulator script on your local machine or another environment. This script emulates user activities such as posting, commenting, and liking, generating data that mimics real user interactions.

    ```bash
    python user_posting_emulation.py
    ```
    This process will continue to run and send data through the API until you interupt it.

4. **Data Routing and Storage**: The incoming data from the emulator is processed by the EC2 instance and then published to Apache Kafka topics within Amazon MSK. Subsequently, it is stored in AWS S3 buckets for durable storage.

**Data Cleaning and Processing in Databricks**: Utilize the provided Databricks notebooks for data cleaning and processing. These notebooks are specifically tailored to work with the data structure generated by the emulator and stored in S3.

Note: You will need to replace the names of the topics referenced in these files with your own. In particular the first notebook **create_dataframes_from_s3**. There are currently cells referencing 3 file paths e.g: 

      file_location = "dbfs:/mnt/0e95b18877fd-S3/topics/0e95b18877fd.pin/partition=0/*.json" 


  - **Data Cleaning Notebook**: Use the `data_cleaning.ipynb` notebook to perform initial data cleaning operations, preparing the data for in-depth analysis.
  
    This notebook also imports and runs the cells in **create_dataframes_from_s3** in the first cell (this filepath may need replacing to reference the location of the file in your data bricks file system):

    ```python
    %run "/Workspace/Repos/adamevansjs@gmail.com/pinterest-data-pipeline684/batch_notebooks/create_dataframes_from_s3"

6. **Workflow Orchestration with Apache Airflow**: Apache Airflow orchestrates the pipeline workflows, scheduling and managing tasks efficiently. modify the provided DAG to automate the execution of the data_cleaning Notebook, ensuring data is processed systematically and on schedule.


### Stream Processing

1. Real-Time Data Generation: Similar to batch processing, start by running the user_posting_emulation_streaming.py script to generate real-time data and send it to AWS Kinesis.

```bash
python user_posting_emulation_streaming.py
```
2. Kinesis Data Streams: Data generated by the emulator is sent directly to pre-configured AWS Kinesis Data Streams, enabling real-time data capture.

3. Processing Data with Databricks and Spark Structured Streaming: Leverage Spark Structured Streaming in Databricks to process the real-time data from Kinesis streams. The notebooks provided are set up to read the stream, perform transformations, and output the processed data for further use.

    - Run the data_cleaning_stream Notebook to read and clean the data. The final 3 cells let you write each stream to a delta table in realtime.

    - You will once again need to change the first %run statement to match the location of the read_stream notebook in your databricks file system.

    - You will need to customise the schema of each stream to fit your incoming stream data. Also change the names of the variables to suit your stream names.

    - If you used different stream names you will need to alter these in the read)stream notebook too. 

### Data Exploration

explanation of what these files illustrate

## Configuration

## Troubleshooting

## Advanced Features

## Contributing

## License

## Acknowledgments

## TO DO

1. Remove all text talking about using a different data source and or topic names and make it all specific otherwise most of the data cleaning and queries will not work. Reference this at the start of the README. Instead provide a guide on how to replicate or find their own 3 talbes to match our data. Check if I am allowed to give out access to our own RDS databsae as part of this project? 
2. finish other readme sections and check that the contents still match the current format. 
3. Should I add subsections with links in the contents for the AWS section? it is quite long currently.