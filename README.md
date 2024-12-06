# Reddit ETL with Airflow, S3, AWS Glue, AWS Lambda, Docker, Athena, Tableau

## Table of Contents

- [Overview](#overview)
- [Architecture Diagram](#architecture-diagram)
- [Prerequisites](#prerequisites)
- [Tableau Example](#tableau-example)

## Overview

Purpose - Posts on the r/OMSCS subreddit must have a post flair assigned to them. However, Reddit does not natively support viewing post statistics by flair. Amidst concerns about post quality and a high volume of "dumb questions" at the beginning of the semester, I wanted to find out how different posts are received by flair. Reddit enforces a limit of 1000 most recent posts per API call so my solution was to setup a weekly Airflow DAG to pull the most recent 1000 posts, deduplicate and aggregate as the base dataset.


This pipeline retrieves post data from a subreddit for analysis in Tableau via these steps:

1. Call Reddit's subreddit API endpoint
2. Extract post data
2. Store the raw data in S3 bucket
3. Clean/Transform data using AWS Lambda triggers
4. Crawl transformed data in S3 using AWS Glue crawler
5. Create a connection to the data using AWS Athena + Glue
6. Connect Tableau to Athena for analysis

## Architecture Diagram
![RedditDataEngineering.png](assets%2Freddit_aws.png)

## Prerequisites
- AWS account with S3, Glue, Lambda, Athena enabled
- Reddit API credentials
- Docker
- Tableau/other Data Viz software with Athena connections
- Python 3.11 or higher
- config/config.conf file containing credentials used by utils.py

## Tableau Example
### Click to enlarge
![RedditAWSTableau.png](assets%2FTableau4Github.png)
