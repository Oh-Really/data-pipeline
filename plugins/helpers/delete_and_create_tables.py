import datetime
import os
from airflow import DAG
import pendulum
from udacity.common import create

from airflow.decorators import dag
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator


@dag(start_date= pendulum.now())
def create_delete():

    tables = [
    "public.staging_events",
    "public.staging_songs",
    "public.artists",
    "public.songs",
    "public.users",
    "public.time",
    "public.songplays"]

    create_tables = [
    ("staging_events", create.staging_events_table_create),
    ("staging_songs", create.staging_songs_table_create),
    ("users", create.user_table_create),
    ("songs", create.song_table_create),
    ("artists", create.artist_table_create),
    ("time", create.time_table_create),
    ("songplays", create.songplay_table_create)
    ]

    # Create empty lists to track tasks for dependency management
    drop_tasks = []
    create_tasks = []

    for table in tables:
        drop_tables_task = PostgresOperator(
        task_id=f"drop_{table}",
        postgres_conn_id="redshift",
        sql=f"DROP TABLE IF EXISTS {table};"
        )
        drop_tasks.append(drop_tables_task)

    for table_name, table_sql in create_tables:
        create_tables_task = PostgresOperator(
            task_id=f"create_{table_name}_table",
            postgres_conn_id="redshift",
            sql=table_sql
        )
        create_tasks.append(create_tables_task)

    for drop_task, create_task in zip(drop_tasks, create_tasks):
        drop_task >> create_task

create_delete_dag = create_delete()
