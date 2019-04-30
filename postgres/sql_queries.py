# DROP TABLES
songplay_table_drop = "DROP TABLE IF EXISTS songplay CASCADE"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE"

# CREATE TABLES
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay (
    songplay_id SERIAL PRIMARY KEY,
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
    gender VARCHAR CHECK (gender in ('M', 'F')),
    level VARCHAR CHECK (level in ('paid', 'free'))
    )
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR,
    artist_id VARCHAR,
    year INT CHECK (year >= 0),
    duration FLOAT CHECK (duration > 5.0)
    )
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY,
    name VARCHAR,
    location VARCHAR,
    latitude FLOAT CHECK (latitude >= -90 AND latitude <= 90),
    longitude FLOAT CHECK (longitude >= -180 AND longitude <= 180)
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
    weekday INT)
""")

# INSERT RECORDS
songplay_table_insert = ("""
INSERT INTO songplay (ts, user_id, level, song_id, artist_id, session_id, location, user_agent)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) 
VALUES (%s, %s, %s, %s, %s)
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) DO UPDATE 
SET location = EXCLUDED.location, latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude
""")

time_table_insert = ("""
INSERT INTO time (ts, hour, day, woy, month, year, weekday)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (ts) DO NOTHING
""")

# FIND SONGS
song_select = ("""
SELECT s.song_id, a.artist_id
FROM songs s JOIN artists a ON s.artist_id = a.artist_id
WHERE s.title = %s AND a.name = %s AND s.duration = %s
""")

# QUERY LISTS
create_table_queries = [time_table_create, artist_table_create, song_table_create, user_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

# QUERYS BY TABLE
song_queries = [song_table_drop, song_table_create, song_table_insert]
artist_queries = [artist_table_drop, artist_table_create, artist_table_insert]
