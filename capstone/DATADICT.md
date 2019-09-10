# Data dictionary

## About
This file contains an overview of the attributes (columns), types and descriptions for all tables used in the project.

### Tables

#### Staging tweets

| Column | Type | description |
| --- | --- | --- 
| user_id | varchar | ID of Twitter user
| name | varchar | "Real" name of Twitter user, e.g. Stefan Langenbach
| nickname | varchar | Screename of Twitter user, e.g. @stefan
| description | varchar | Bio of Twitter user
| user_location | varchar | Location of Twitter user, e.g. Milan, Italy
| followers_count | int | Number of followers of Twitter user
| tweets_count | int | Number of tweets of Twitter user
| user_date | timestamp | Date Twitter user was created
| verified | bool | Whether the user is verified (small blue check sign)
| tweet_id | varchar | ID of tweet
| text | varchar | Text of tweet
| favs | int | Number of times tweet is marked as favourite by other Twitter users
| retweets | int | Number of times tweet is retweeted by other Twitter users
| tweet_date | timestamp | Date tweet was created
| tweet_location | varchar | Location the tweet was send from (if available)
| source | varchar | Device/App used to create the tweet, e.g. Twitter for Android/iPhone, Twitter for Desktop, etc.
| sentiment | varchar | Sentiment of the tweet's text as determined by AWS Comprehend (positive/neutral/negative)

### Staging happiness

| Column | Type | description |
| --- | --- | --- 
| country | varchar | Country happiness data has been collected on
| rank | int | Position of country in happiness ranking
| score | numeric | Happiness score
| confidence_high | numeric | Upper confidence interval for happiness score
| confidence_low | numeric | Lower confidence interval for happiness score
| economy | numeric | Contribution of economic situation to happiness score (

### Staging temperatures
