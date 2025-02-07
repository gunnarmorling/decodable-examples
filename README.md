# Decodable Examples

This repository contains examples of use cases that utilize Decodable streaming solution as well as demos for related open-source projects such as Apache Flink, Debezium, and Postgres.

## Contents

| Example | Description |
|---------|-------------|
| [Flink Learn](./flink-learn/) | Apache Flink tutorials and webinar|
| [AsyncAPI](asyncapi) | Publishing Data Products with AsyncAPI |
| [Opinionated Data Pipelines](opinionated-pipelines) | Building data pipelines with schema on write streams. |
| [Postman](postman) | Building data pipelines with Postman. |
| [Change Streams](change-streams) | Using change streams to build materialized views in Postgres |
| [XML Processing](xml) | Parse XML and transform to JSON |
| [OSQuery Routing](osquery) | Route OSQuery logs with SQL |
| [Masking](masking) | Ways to mask data |
| [Apache Pinot](pinot) | Transforming osquery logs to Apache Pinot and Superset |
| [Apache Druid](druid) | This example sends covid 19 data to Decodable using it's REST API. The data is then cleansed using Decodable SQL and send the data to a Kafka sink.  |
| [Rockset](rockset) | We will be utilizing a cloud MQTT broker and AWS Kinesis to capture and stream data. Decodable will be responsible for preparing and aggregating the data prior to reaching the real-time analytical database (Rockset) |
| [Tinybird](tinybird) | We write data to Tinybird and build a simple real time web application. |
| [Apache Kafka](kafka2s3) | Installing Apache Kafka on EC2 and writing to S3 with Decodable |
| [Apache Kafka mTLS](mtls) | We install Apache Kafka on EC2 and configure it with mTLS and configure Decodable to read from it |
| [Snowflake + Snowpipe](snowflake) | We setup a snowpipe at the end of a Decodable S3 sink. |
| [Confluent](confluent) | Clickstream from Confluent Cloud joined with CDC user data from Postgres |
| [Snowflake + Snowpipe + Merge](snowflake/README-CDC.md) | Leveraging Snowpipe, we send CDC data from Postgres to be processed in Snowflake using an Append Only Stream in Snowflake to merge CDC data in a Snowflake table. Essentially `mirroring` the table in Postgres in a Snowflake table. |
|[Reading S3 Events in a Lambda Function](s3events/)|We configure an S3 bucket with a Lambda notification to send data to Kinesis to be processed in Decodable |
|[MSSQL CDC](mssql_cdc/)| We enable a MSSQL in Docker with CDC. We then stand up a Debezium server to read from MSSQL and write the change events into AWS Kinesis  |
|[Oracle CDC](oracle_cdc/)| We configure a Oracle AWS RDS with LogMiner. We then stand up a Debezium server to read change events into AWS Kinesis  |
|[DynamoDb CDC](dynamodb_cdc/)| We configure a DynamoDB to send change data to Kinesis. Then we read those changes into Decodable for transformation or replication.  |
|[ Logical Decoding Message Examples](postgres-logical-decoding)| We show how to retrieve logical decoding messages from the Postgres WAL |

## License

This code base is available under the Apache License, version 2.

