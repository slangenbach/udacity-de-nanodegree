# Data modelling data with PostgreSQL

## About
The scripts within this repository perform ETL (extract, transform, load) for
song- and log data found in the context of a music-streaming service (Sparkify).
After target tables have been created (c.f. ```create_tables.py```), data stored
on the local filesystem in JSON-format is loaded from the data directory as 
pandas Dataframes, processed (c.f. ```etl.py```) and inserted into a PostgreSQL
database (c.f. ```sql_queries.py```). The code in ```helpers.py``` contains just
some functions that were used during development of the ETL-process (c.f. ```etl.ipynb```).


After the scripts have been executed, analysts will be able to query fact 
and dimensions tables to gain insights about songs, artists, users and the songs they
have listened to. In particular, the following tables will be created:

| table | columns |
--- | ---
| songs | song_id, title, artist_id, year, duration |
| artists | artist_id, name, location, latitude, longitude |
| time | ts (timestamp), hour, day, woy (week of year), month, year, weekday |
| users | user_id, first_name, last_name, gender, level |
| songplay | songplay_id, ts (timestamp), user_id, level, song_id, artist_id, session_id, location, user_agent |

## Prerequisites
* Access to a PostgreSQL database with create/drop/insert/update rights
* Python 3.6+
* Python packages psycopg2 and pandas
* Unix-like environment (Linux, macOS, WSL on Windows)

## Usage
Create all required tables - tables will be dropped if they already exist
```
python create_tables.py
```
Process song- and log data
```
python etl.py
```

## Limitations
* Although database tables do require specific types and enforce a few checks (c.f. ```sql_queries.py```),
when upsering data, not test suite for the ETL script exists yet.

## Ressources
* [PostgreSQL constraints](https://www.postgresql.org/docs/current/ddl-constraints.html)
* [PostgreSQL inserts](https://www.postgresql.org/docs/current/sql-insert.html)
* [PostgreSQL CREATE TABLE command](https://www.postgresql.org/docs/current/sql-createtable.html)
* [PostgreSQL COPY command](https://www.postgresql.org/docs/current/sql-copy.html)