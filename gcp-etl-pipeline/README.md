# Developing ETL Pipeline in Google Cloud Platform

Purpose:

The purpose of this project was to build an ETL pipeline for the given(Confidential) dataset on Google Cloud Platform(GCP) to perform the extraction, transformation, and loading(ETL) of data and then transfer into BigQuery for analytics purposes.

Approach

1. Use GCP Deployment Manager to create necessary resources like GCS buckets, BigQuery tables, and a virtual machine.

2. Install Apache NiFi on the virtual machine to extract data from the SQL server and dump it into a GCS bucket.

3. Create a cloud function to monitor the bucket for changes and trigger a PySpark job using Cloud Dataproc.

4. Utilize workflow templates to create a Dataproc cluster and execute the PySpark transformation job.

5. Load the transformed data into BigQuery for analytics and optionally store a backup in a cloud storage bucket.

Tech Stack

Language: Python, SQL

Services: SQL Server, AWS RDS, GCP Compute Engine, GCP Cloud Functions, Apache NiFi, GCP Cloud Storage, GCP BigQuery, GCP Dataproc, GCP Deployment Manager