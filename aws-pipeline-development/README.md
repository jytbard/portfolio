# Developing ETL Pipeline in Google Cloud Platform

Agenda

A Data Pipeline was built using Apache NiFi, Apache Spark, AWS S3, Amazon EMR cluster, Amazon OpenSearch, Logstash and Kibana. The data was fetched from an API(https://www.athenahealth.com/) using Apache NiFi, transformed and loaded it in an AWS S3 bucket. Using Logstash the data was ingested from an AWS S3 bucket into Amazon OpenSearch. From Amazon OpenSearch data was passed into Kibana to perform Data visualization on data. 

Tech stack: 

➔Language: Python

➔Package: Pyspark

➔Services: AWS NiFi, AWS EC2, Apache Spark, AWS S3, Amazon EMR cluster, Amazon OpenSearch, Logstash, Kibana