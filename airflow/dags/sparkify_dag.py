from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)


# specify DAG default arguments
default_args = {
    'owner': 'sparkify',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
    'aws_conn_id': 'aws_default',
    'postgres_conn_id': 'redshift_default',
    'redshift_conn_id': 'redshift_default',
    'params': {
        's3_bucket': 'udacity-dend',
        's3_region': 'us-west-2',
        's3_json_path': 's3://udacity-dend/log_json_path.json',
        'aws_iam_role': Variable.get("aws_iam_role")
    }
}

# define DAG
with DAG(dag_id="sparkify_dag",
         description="Load and transform data from S3 into Redshift",
         default_args=default_args,
         schedule_interval="@hourly",
         template_searchpath=str(Path(__file__).parent.parent.joinpath("sql"))) as dag:

    # define tasks
    start = DummyOperator(task_id='begin_execution')

    create_tables = PostgresOperator(
        task_id="create_tables",
        sql="create_tables.sql"
    )

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="stage_events",
        aws_iam_role="{{ params.aws_iam_role }}",
        s3_bucket="{{ params.s3_bucket }}",
        s3_key="log-data",
        s3_region="{{ params.s3_region }}",
        redshift_table="staging_events",
        json_path="{{ params.s3_json_path }}",
        truncate=False
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="stage_songs",
        aws_iam_role="{{ params.aws_iam_role }}",
        s3_bucket="{{ params.s3_bucket }}",
        s3_key="song-data",
        s3_region="{{ params.s3_region }}",
        redshift_table="staging_songs",
        json_path="auto",
        truncate=False
    )

    load_songplays_table = LoadFactOperator(
        task_id="load_songplay_fact_table",
        fact_table="songplay",
        fact_cols="playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent",
        sql="songplay_insert.sql",
        truncate=False
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='load_user_dim_table',
        dim_table="users",
        dim_cols="userid, first_name, last_name, gender, 'level'",
        sql="user_insert.sql",
        truncate=False
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='load_song_dim_table',
        dim_table="songs",
        dim_cols="songid, title, artistid, 'year', duration",
        sql="song_insert.sql",
        truncate=False
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='load_artist_dim_table',
        dim_table="artists",
        dim_cols="artistid, name, location, latitude, longitude",
        sql="artist_insert.sql",
        truncate=False
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='load_time_dim_table',
        dim_table="time",
        dim_cols="start_time, 'hour', 'day', week, 'month', 'year', weekday",
        sql="time_insert.sql",
        truncate=False
    )

    run_quality_checks = DataQualityOperator(
        task_id='run_data_quality_checks',
        check_tables=["songplay", "users", "songs", "artists", "time"],
        check_sql="has_rows"
    )

    end = DummyOperator(task_id='Stop_execution')

    # define task dependencies
    staging_tasks = [stage_events_to_redshift, stage_songs_to_redshift]
    load_dim_tables = [load_song_dimension_table, load_artist_dimension_table, load_user_dimension_table,
                       load_time_dimension_table]

    start >> create_tables >> staging_tasks >> load_songplays_table >> load_dim_tables >> run_quality_checks >> end
