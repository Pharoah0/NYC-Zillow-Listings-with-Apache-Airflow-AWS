# NYC Zillow Listings with Apache Airflow & AWS
Author: Pharoah Evelyn
## Overview
#### This repository outlines the curation of a data pipeline that pulls data from a Zillow API into AWS using Apache Airflow. 
Once inside an S3 bucket, we employ Lambda functions to run our transformations and utilize QuickSight for data visualization. 

## Business Problem
A Real-Estate agency is curious about

## Data Preparation
I Utilized Rapid API to employ a web scraper on Zillow for listing within the NYC area.

The API is what's responsible for retrieving our data. I did this in JSON format for demonstration purposes, but CSV was also possible.

## Methods Used
I incorporated this API within an Airflow DAG, which stored the data pulled locally onto my EC2 server and then copied the data onto an S3 bucket.

I configured Lambda functions to trigger based on S3 PutObject activities: 
- Function #1 reacts to the raw S3 bucket, transforms that data into Parquet format, and places it into a second transformed S3 bucket
- Function #2  reacts to the transformed S3 bucket, triggering a Glue Crawler to crawl the bucket and catalog the data from all files for visualization.

## Discoveries made


## Ways to improve this project
We can go entirely serverless; instead of using Airflow on an EC2 instance, we can utilize Amazon Managed Workflows for Apache Airflow - a serverless solution for operating Airflow within the cloud.

Furthermore, we could also use Airflow to orchestrate different AWS services within DAGs for this scenario if we wanted to, instead of using Lambda functions.

## Conclusions