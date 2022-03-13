![Scala](https://img.shields.io/badge/scala-%23DC322F.svg?style=for-the-badge&logo=scala&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-000?style=for-the-badge&logo=apachekafka)
![MySQL](https://img.shields.io/badge/mysql-%2300f.svg?style=for-the-badge&logo=mysql&logoColor=white)
![AWS](https://img.shields.io/badge/AWS-%23FF9900.svg?style=for-the-badge&logo=amazon-aws&logoColor=white)
<br/>

# spark-realtime-streaming-emr-5.34
This project is a real-time streaming data pipeline utilized with the ScalaSpark

## Table of contents
* [Introduction](#introduction)
* [Technologies](#technologies)
* [Setup](#setup)
* [Workflow](#workflow)
* [Sources](#sources)

## Introduction
This project is a real-time streaming data pipeline utilized with the ScalaSpark artifact above. NIFI implements pulling TTC bus route data and connects to Kafka
![DIAGRAM](https://github.com/ChloeHeekSuh/spark-realtime-streaming-emr-5.34/blob/master/screenshot/diagram.png)

## Technologies
* Spark Version = 2.4.4
* Scala Version = 2.11.11
* MySQL-Debezium Version = 1.5
* NIFI Version = 1.12.0
* Kafka Version = 2.6.2

## Setup
The explaination below is simplified steps. We will use debezium to interact NIFI, Mysql and kafka each other.

1. Setup Docker on AWS EC2\
: [Docker basics for Amazon ECS](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/docker-basics.html)

2. Install MySQL and NIFI on Docker
```
docker run -dit --name mysql -p 3306:3306 -e MYSQL_ROOT_PASSWORD=debezium -e MYSQL_USER=mysqluser -e MYSQL_PASSWORD=mysqlpw debezium/example-mysql:1.6
```
```
docker run --name nifi -p 8080:8080 -p 8443:8443 --link mysql:mysql -d apache/nifi:1.12.0
```
3. Connect the TTC Rest API and MySQL to NIFI
4. Install Zookeeper and Kafka
```
docker run -it --name zookeeper -p 2181:2181 -p 2888:2888 -p 3888:3888 debezium/zookeeper
```
```
docker run -dit --name kafka -p 9092:9092 --link zookeeper:zookeeper debezium/kafka:1.6
```
5. Run a Debezium connect command to link each other including MySQL and to create a JSON that will be used for a data structure.
```
docker run -dit --name connect -p 8083:8083 -e GROUP_ID=1 -e CONFIG_STORAGE_TOPIC=my-connect-configs -e OFFSET_STORAGE_TOPIC=my-connect-offsets -e STATUS_STORAGE_TOPIC=my_connect_statuses --link zookeeper:zookeeper --link kafka:kafka --link mysql:mysql debezium/connect:1.6
```
6. Upload the artifact to the S3 bucket
7. Run the MSK and EMR cluster


## Workflow

Extract bus route data from [TTC Rest API](http://restbus.info/api/agencies/ttc/routes/7/vehicles) through the NIFI. The NIFI makes automation for the movement of data between systems. So in this case, Nifi collects the bus status and files this data to the Mysql database.
<img src="https://github.com/ChloeHeekSuh/spark-realtime-streaming-emr-5.34/blob/master/screenshot/nifi.png">
<img src="https://github.com/ChloeHeekSuh/spark-realtime-streaming-emr-5.34/blob/master/screenshot/mysql.png">          
Debezium utilizes CDC, to capture the change of the data. For achieving the universe, Debezium connect is going to create the first snapshot in MySQL table and download all the previous records from MySQL first, load them into Kafka and repeat so on to update.
 
After that, spark-submit starts data processing with Hudi on the AWS EMR cluster to convert the data to a parquet file. More specifically, existing records are documented in metadata(commit file) in Hudi.

After updating the records, the new records are saved to new parquets. Also, a new commit is being a new universe.

<img src="https://github.com/ChloeHeekSuh/spark-realtime-streaming-emr-5.34/blob/master/screenshot/athena.png" width="900">
Those parquets will be saved to the S3 bucket and also Athena set up the table with the parquet as I defined on the spark Jar. The reason why using Athena is to query the Hudi data that read the data all of in MSK directly.

## Sources
This app is inspired by Edwin Guo’s lecture and [Spark Streaming with Kafka-Connect Debezium Connector by @Suchit Gupta](https://suchit-g.medium.com/spark-streaming-with-kafka-connect-debezium-connector-ab9163808667)
