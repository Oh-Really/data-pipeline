from datetime import datetime, timedelta
import pendulum
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'depends_on_past' : False,
    'retries' : 3,
    'retry_delay' : timedelta(minutes=5),
    'email_on_retry' : False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    
    #Copy json logs from S3 to staging events table
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events_to_redshift',
        redshift_conn_id = "redshift",
        aws_credentials_id = "aws_credentials",
        table = "staging_events",
        s3_bucket = "mo-pipelines",
        s3_key = "log-data",
        data_format = "log_json_path.json",
    )


    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs_to_redshift',
        redshift_conn_id = "redshift",
        aws_credentials_id = "aws_credentials",
        table = "staging_songs",
        s3_bucket = "mo-pipelines",
        s3_key = "song-data",
        data_format = "auto",
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id = "redshift",
        table = 'songplays',
        sql_stmnt = SqlQueries.songplay_table_insert,
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id = "redshift",
        table = 'users',
        sql_stmnt = SqlQueries.user_table_insert,
        truncate_table=True
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id = "redshift",
        table = 'songs',
        sql_stmnt = SqlQueries.song_table_insert,
        truncate_table=True
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id = "redshift",
        table = 'artists',
        sql_stmnt = SqlQueries.artist_table_insert,
        truncate_table=True
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id = "redshift",
        table = 'time',
        sql_stmnt = SqlQueries.time_table_insert,
        truncate_table=True
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id = "redshift",
        checks = [
        { 'check_sql': 'SELECT COUNT(*) FROM public.songplays WHERE userid IS NULL', 'expected_result': 0 }, 
        { 'check_sql': 'SELECT COUNT(DISTINCT "level") FROM public.songplays', 'expected_result': 2 },
        { 'check_sql': 'SELECT COUNT(*) FROM public.artists WHERE name IS NULL', 'expected_result': 0 },
        { 'check_sql': 'SELECT COUNT(*) FROM public.songs WHERE title IS NULL', 'expected_result': 0 },
        { 'check_sql': 'SELECT COUNT(*) FROM public.users WHERE first_name IS NULL', 'expected_result': 0 },
        { 'check_sql': 'SELECT COUNT(*) FROM public."time" WHERE weekday IS NULL', 'expected_result': 0 },
        { 'check_sql': 'SELECT COUNT(*) FROM public.songplays sp LEFT OUTER JOIN public.users u ON u.userid = sp.userid WHERE u.userid IS NULL', \
         'expected_result': 0 }
        ]
    )

    end_operator = DummyOperator(task_id='End_execution')

    start_operator >> stage_events_to_redshift
    start_operator >> stage_songs_to_redshift

    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table

    load_songplays_table >> load_user_dimension_table
    load_songplays_table >> load_song_dimension_table
    load_songplays_table >> load_artist_dimension_table
    load_songplays_table >> load_time_dimension_table

    load_user_dimension_table >> run_quality_checks
    load_song_dimension_table >> run_quality_checks
    load_artist_dimension_table >> run_quality_checks
    load_time_dimension_table >> run_quality_checks

    run_quality_checks >> end_operator

final_project_dag = final_project()
