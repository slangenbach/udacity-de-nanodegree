# cluster setup
cluster_create_keyspace = """
CREATE KEYSPACE IF NOT EXISTS sparkify WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor' : 3}
"""

# analytic queries
# query 1
q1_drop_table = """
DROP TABLE IF EXISTS song_playlist_session
"""

q1_create_table = """
CREATE TABLE IF NOT EXISTS song_playlist_session (
    sessionId int,
    itemInSession int,
    artist text,
    song text,
    length float,
    PRIMARY KEY (sessionId, itemInSession)
)
"""

q1_query = """
SELECT artist, song, length
FROM song_playlist_session
WHERE sessionId = 338 AND itemInSession = 4
"""

# query 2
q2_drop_table = """
DROP TABLE IF EXISTS song_playlist_user
"""

# https://www.datastax.com/dev/blog/we-shall-have-order
q2_create_table = """
CREATE TABLE IF NOT EXISTS song_playlist_user (
    userId float,
    sessionId int,
    itemInSession int,
    artist text,
    song text,
    firstName text,
    lastName text,
    PRIMARY KEY ((userId, sessionId), itemInSession)
) 
WITH CLUSTERING ORDER BY (itemInSession DESC)
"""

q2_query = """
SELECT artist, song, firstName, lastName, itemInSession
FROM song_playlist_user
WHERE userId = 10 AND sessionId = 182
"""

# query 3
q3_drop_table = """
DROP TABLE IF EXISTS song_user_name
"""

q3_create_table = """
CREATE TABLE IF NOT EXISTS song_user_name (
    song text,
    userId float,
    firstName text,
    lastName text,
    PRIMARY KEY (song, userId)
)
"""

q3_query = """
SELECT firstName, lastName
FROM song_user_name
WHERE song = 'All Hands Against His Own'
"""