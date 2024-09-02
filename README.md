# Reddit ETL with Airflow, S3, AWS Glue, AWS Lambda, Docker, Athena, Tableau

## Table of Contents

- [Overview](#overview)
- [Diagram](#diagram)
- [Prerequisites](#prerequisites)
- [Notes](#notes)

## Overview

This pipeline retrieves post data from a subreddit for analysis in Tableau via these steps:

1. Call Reddit's subreddit API endpoint
2. Extract post data
2. Store the raw data in S3 bucket
3. Clean/Transform data using AWS Lambda triggers
4. Crawl transformed data in S3 using AWS Glue crawler
5. Create a connection to the data using AWS Athena + Glue
6. Connect Tableau to Athena for analysis

## Diagram
![RedditDataEngineering.png](assets%2Freddit_aws.png)

## Prerequisites
- AWS account with S3, Glue, Lambda, Athena enabled
- Reddit API credentials
- Docker
- Tableau/other Data Viz software with Athena connections
- Python 3.11 or higher

## Example Tableau
![RedditAWSTableau.png](assets%2FTableau4Github.png)
