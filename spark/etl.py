import configparser
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek
from pyspark.sql.functions import monotonically_increasing_id


def create_spark_session(spark_jars: str) -> SparkSession:
    """
    Create Spark session
    :param spark_jars: Hadoop-AWS JARs
    :return: SparkSession
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", spark_jars) \
        .appName("Sparkify ETL") \
        .getOrCreate()
    return spark


def process_song_data(spark: SparkSession, input_data: str, output_data: str) -> None:
    """
    Given an input path to song data, select relevant columns for songs and artist tables and save those to disk
    respecting an output path.
    :param spark: SparkSession
    :param input_data: Path to input data
    :param output_data: Path to store output data
    :return: None
    """
    # get filepath to song data file
    song_data = f"{input_data}/song_data/*/*/*"

    # read song data file
    print("Loading song data")
    df = spark.read.format("json").load(song_data)

    # extract columns to create songs table
    songs_table = df.dropDuplicates(["song_id"]).select(["song_id", "title", "artist_id", "year", "duration"])

    # write songs table to parquet files partitioned by year and artist
    print("Writing songs table")
    songs_table.write.partitionBy(["year", "artist_id"]).parquet(f"{output_data}/songs/", mode="overwrite")

    # extract columns to create artists table
    artists_table = df.dropDuplicates(["artist_id"]).select(["artist_id", "artist_name", "artist_location",
                                                             "artist_latitude", "artist_longitude"])

    # write artists table to parquet files
    print("Writing artists table")
    artists_table.write.parquet(f"{output_data}/artists/", mode="overwrite")
    print("Finished processing song data")


def process_log_data(spark: SparkSession, input_data: str, output_data: str) -> None:
    """
    Given an input path to log data, select relevant columns for user and time tables and save those to disk
    respecting an output path. Then load previously processed song and artist data, join it with log data, create
    the songplay table and write it to disk.
    :param spark: SparkSession
    :param input_data: Path to input data
    :param output_data: Path to store output data
    :return: None
    """
    # get filepath to log data file
    log_data = f"{input_data}/log_data/*/*/"

    # read log data file
    print("Loading log data")
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table
    user_table = df.dropDuplicates(["userId"]).select(["userId", "firstName", "lastName", "gender", "level"])

    # write users table to parquet files
    print("Writing user table")
    user_table.write.parquet(f"{output_data}/users/", mode="overwrite")

    # create timestamp column from original timestamp column
    df = df.withColumn("ts", from_unixtime(df.ts / 1000))

    # extract columns to create time table
    time_table = df.dropDuplicates(["ts"]).select(["ts",
                                                   hour(df.ts).alias("hour"),
                                                   dayofmonth(df.ts).alias("day"),
                                                   weekofyear(df.ts).alias("week"),
                                                   month(df.ts).alias("month"),
                                                   year(df.ts).alias("year"),
                                                   dayofweek(df.ts).alias("weekday")])

    # write time table to parquet files partitioned by year and month
    print("Writing time table")
    time_table.write.partitionBy(["year", "month"]).parquet(f"{output_data}/time/", mode="overwrite")

    # read in song and artist data required for songplays table
    print("Loading song data")
    songs_table = spark.read.parquet(f"{output_data}/songs/")

    print("Loading artist data")
    artists_table = spark.read.parquet(f"{output_data}/artists/")

    # join datasets
    print("Joining song, artist and log data")
    join_cond = [df.song == songs_table.title, df.artist == artists_table.artist_name,
                 df.length == songs_table.duration]

    joined_df = songs_table.join(artists_table, "artist_id", "inner").join(df, join_cond, "inner")

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = joined_df \
        .withColumn("songplay_id", monotonically_increasing_id()) \
        .withColumn("year", year(df.ts).alias("year")) \
        .withColumn("month", month(df.ts).alias("month")) \
        .select(["songplay_id", "ts", "userId", "level", "song_id", "artist_id", "sessionId",
                 "artist_location", "userAgent", "year", "month"])

    # write songplays table to parquet files partitioned by year and month
    print("Writing songplay table")
    songplays_table.write.partitionBy(["year", "month"]).parquet(f"{output_data}/songplays/", mode="overwrite")
    print("Finished processing log data")


def main():
    # load config
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    # define environment variables for AWS EMR
    os.environ['AWS_ACCESS_KEY_ID'] = config.get("AWS", "AWS_ACCESS_KEY_ID")
    os.environ['AWS_SECRET_ACCESS_KEY'] = config.get("AWS", "AWS_SECRET_ACCESS_KEY")
    os.environ['SPARK_HOME'] = config.get("EMR", "SPARK_HOME")
    os.environ['PYSPARK_PYTHON'] = config.get("EMR", "PYSPARK_PYTHON")
    sys.path.append(config.get("EMR", "PYSPARK_PYTHON"))
    sys.path.append(config.get("EMR", "PY4J"))

    # define input/output paths for loading/writing data
    input_data = config.get("ETL", "INPUT_DATA")
    output_data = config.get("ETL", "OUTPUT_DATA")

    # create spark session
    print("Start pipeline")
    print("Creating Spark Session")
    spark = create_spark_session(config.get("EMR", "SPARK_JARS"))

    # process data
    print("Start processing song data")
    process_song_data(spark, input_data, output_data)

    print("Start processing log data")
    process_log_data(spark, input_data, output_data)

    print("Finished pipeline")


if __name__ == "__main__":
    main()
