INSERT INTO time ("date", "second", "minute", "hour", week, "month", "year", weekday)
SELECT tweet_date,
       extract(second from "date"),
       extract(minute from "date"),
       extract(minute from "date"),
       extract(week from "date"),
       extract(month from "date"),
       extract(year from "date"),
       extract(dayofweek from "date")
FROM staging_tweets;
