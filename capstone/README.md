# Capstone project

## About
Ideas:
* Work with batch- and streaming data from Twitter
* Stream data on current topic, e.g. [#FridaysForFuture](https://twitter.com/hashtag/FridaysForFuture)
* Combine stream data with static dataset(s), e.g. [Climate Change: Earth Surface Temperature](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data) 
from [Kaggle](https://www.kaggle.com/datasets) or 
[National Footprint Accounts](https://data.world/footprint/nfa-2017-edition) from [Data World](https://data.world)
* Use AWS tools, e.g. Functions, Kinesis, S3, Redshift/Athena/EMR, to process, store and analyze data
* Set up AWS tools using [Ansible](https://docs.ansible.com/ansible/latest/index.html)

Data
* https://www.kaggle.com/unsdsn/world-happiness
* https://data.worldbank.org/

Tasks:
* Identify data sources (at least 2 sources containing >1 million rows)
* List potential use cases for selected data
    - Calculate summary statistics per day/week/month/year
    - Answer questions regarding tweets
    - Use static dataset to explore questions regarding potential relations between climate change / footprint of nation 
    and origin of tweet
* Assess data for quality issues such as missing values, duplicates, etc.
* Document steps required to clean data
* Define data model and explain your choice
* Define pipeline to fill model with data
* Create data dictionary
* Create pipeline including data quality checks
* Complete documentation
    - Define overall goal of data processing, c.f. use cases
    - Justify technologies used during the project
    - Include overview or data wrangling process, i.e. airflow process
    - Mention reasons and interval for updating data
    - Discuss alternative solution given 100x increase in data, data pipelines required to run every day in the morning, 
    and simultaneous access by > 100 users

## Prerequisites
* Ansible
* Python packages tweepy, boto3 (c.f. capstone_env.yml and requirements.txt)

## Usage
tbd

## Limitations
tbd

## Resources
* [Twitter API documentation](https://developer.twitter.com/en/docs)
* [Twitter search API documentation](https://developer.twitter.com/en/docs/tweets/search/api-reference/get-search-tweets.html)
* [Twitter tweet data dictionary](https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/tweet-object)
* [Twitter basic stream API parameters documentation](https://developer.twitter.com/en/docs/tweets/filter-realtime/guides/basic-stream-parameters)
* [Tweepy documentation](https://tweepy.readthedocs.io/en/latest/index.html)
* [Tutorial on using tweepy to process Twitter streams with Python](https://www.dataquest.io/blog/streaming-data-python/)
* [Blog post on using Kinesis and Redshift to stream Twitter data](https://medium.com/@siprem/streaming-twitter-feed-using-kinesis-data-firehose-and-redshift-745c96d04f58)
* [Blog post on using Kinesis to batch insert streaming data](https://medium.com/retailmenot-engineering/building-a-high-throughput-data-pipeline-with-kinesis-lambda-and-dynamodb-7d78e992a02d)
* [AWS Kinesis documentation for PutRecords](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html)