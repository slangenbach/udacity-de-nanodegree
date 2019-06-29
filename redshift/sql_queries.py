import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events CASCADE"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs CASCADE"
songplay_table_drop = "DROP TABLE IF EXISTS songplay CASCADE"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE"
song_table_drop = "DROP TABLE IF EXISTS song CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS artist CASCADE"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE"

# CREATE TABLES
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR,
    auth VARCHAR,
    first_name VARCHAR,
    gender VARCHAR,
    item_in_session INT,
    last_name VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    http_method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    session_id INT,
    song VARCHAR,
    http_status INT,
    ts BIGINT,
    user_agent VARCHAR,
    user_id INT 
)
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
    song_id VARCHAR,
    num_songs INT,
    title VARCHAR,
    artist_name VARCHAR,
    artist_latitude FLOAT,
    year INT,
    duration FLOAT,
    artist_id VARCHAR,
    artist_longitude FLOAT,
    artist_location VARCHAR
    )
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay (
    songplay_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    ts TIMESTAMP REFERENCES time,
    user_id INT REFERENCES users,
    level VARCHAR NOT NULL,
    song_id VARCHAR REFERENCES songs,
    artist_id VARCHAR REFERENCES artists,
    session_id INT NOT NULL,
    location VARCHAR NOT NULL,
    user_agent VARCHAR NOT NULL
    )
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    level VARCHAR
    )
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR PRIMARY KEY,
     title VARCHAR,
     artist_id VARCHAR,
     year INT,
     duration FLOAT
    )
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY,
    name VARCHAR,
    location VARCHAR,
    latitude FLOAT ,
    longitude FLOAT
    )
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    ts TIMESTAMP PRIMARY KEY,
    hour INT,
    day INT,
    woy INT,
    month INT,
    year INT,
    weekday INT
    )
""")

# STAGING TABLES
staging_events_copy = ("""
COPY staging_events FROM '{}'
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
COMPUPDATE OFF
JSON '{}'
""").format(config.get("S3", "LOG_DATA"), config.get("IAM_ROLE", "ARN"), config.get("S3", "LOG_JSONPATH"))

staging_songs_copy = ("""
COPY staging_songs FROM '{}'
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2' 
COMPUPDATE OFF STATUPDATE OFF
JSON 'auto' TRUNCATECOLUMNS
""").format(config.get("S3", "SONG_DATA"), config.get("IAM_ROLE", "ARN"))

# FINAL TABLES
songplay_table_insert = ("""
INSERT INTO songplay (ts, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second' AS valid_ts,
       se.user_id,
       se.level,
       sa.song_id,
       sa.artist_id,
       se.session_id,
       se.location,
       se.user_agent
FROM staging_events se
JOIN (
    SELECT s.song_id, a.artist_id, s.title, a.name, s.duration
    FROM songs s
    JOIN artists a
    ON s.artist_id = a.artist_id
    ) AS sa
ON (
    sa.title = se.song
    AND sa.name = se.artist
    AND sa.duration = se.length
    )
WHERE se.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT user_id, first_name, last_name, gender, level
FROM staging_events
WHERE user_id IS NOT NULL
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time (ts, hour, day, woy, month, year, weekday)
SELECT  valid_ts,
        EXTRACT(hour FROM  valid_ts) AS valid_hour,
        EXTRACT(day FROM valid_ts) AS valid_day,
        EXTRACT(week FROM valid_ts) AS valid_woy,
        EXTRACT(month FROM valid_ts) AS valid_month,
        EXTRACT(year FROM valid_ts) AS valid_year,
        EXTRACT(dow FROM valid_ts) AS valid_weekday
        FROM (
            SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS valid_ts
            FROM staging_events
            ) stg_events
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop, songplay_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]