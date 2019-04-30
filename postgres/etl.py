import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur: psycopg2.extensions.cursor, filepath: str):
    """
    Given a connection (cursor) to a PostgreSQL database and a path to a JSON-(song)-file,
    load the file, and insert valid subsets of its data into song and artist tables.
    
    :param cur: Cursor
    :param filepath: Path to JSON file
    """
    # open song file
    df = pd.read_json(filepath, orient="records", typ="series")

    # insert song record
    song_data = df[["song_id", "title", "artist_id", "year", "duration"]]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur: psycopg2.extensions.cursor, filepath: str):
    """
    Given a connection (cursor) to a PostgreSQL database and a path to a JSON-(log)-file,
    load the file, filter its content, derive datetime attributse from the timestamp column
    and insert valid subsets of its data into the time, user and songplay tables.
    
    :param cur: Cursor
    :param filepath: Path to JSON file
    """
    # open log file
    df = pd.read_json(filepath, orient="records", lines=True)

    # filter by NextSong action
    df = df.loc[(df["page"] == "NextSong"), :]

    # convert timestamp column to datetime
    df["ts"] = df["ts"].astype("datetime64[ms]")
    
    # insert time data records
    time_dict = {
    "timestamp": df.ts,
    "hour": df.ts.dt.hour,
    "day": df.ts.dt.day,
    "week_of_year": df.ts.dt.weekofyear,
    "month": df.ts.dt.month,
    "year": df.ts.dt.year,
    "weekday": df.ts.dt.weekday
    }
    
    time_df = pd.DataFrame.from_dict(time_dict)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur: psycopg2.extensions.cursor, conn: psycopg2.extensions.connection,
                 filepath: str, func):
    """
    Given a connection to a PostgresSQL database, a path to a directory on the
    local filesystem and a processing function, do the following:
        1. Load all *.json files found in filepath and its subdirectories
        2. Print the number of files found in step one
        3. Apply the processing function func to all files found in step one
        
    :param cur: Cursor
    :param conn: Connection to PostgreSQL database
    :param filepath: Path to JSON file
    :param func: (Python) function to process data
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Connect to a PostgreSQL database and process/insert all data returned by process_data
    function.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()