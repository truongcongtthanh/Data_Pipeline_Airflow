from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from airflow.operators import PostgresOperator
#from airflow.providers.postgres.operators.postgres import PostgresOperator
from helpers import SqlQueries



default_args = {
    'owner': 'udacity',
    'start_date': datetime(2022, 2, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}



dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs=1
        )



start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)



create_tables = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql="create_tables.sql"
)



stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    copy_json_option='s3://udacity-dend/log_json_path.json',
    region='us-west-2',
    dag=dag
)



stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    copy_json_option='auto',
    region='us-west-2',
    dag=dag
)



load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id='redshift',
    table='songplays',
    query_insert=SqlQueries.songplay_table_insert,
    dag=dag
)



load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id='redshift',
    table='users',
    query_insert=SqlQueries.user_table_insert,
    dag=dag
)



load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id='redshift',
    table='songs',
    query_insert=SqlQueries.song_table_insert,
    dag=dag
)



load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id='redshift',
    table='artists',
    query_insert=SqlQueries.artist_table_insert,
    dag=dag
)



load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id='redshift',
    table='time',
    query_insert=SqlQueries.time_table_insert,
    dag=dag
)



run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id='redshift',
    test_query='select count(*) from songs where songid is null;',
    expected_result=0,
    dag=dag
)



end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)



start_operator >> create_tables
create_tables >> stage_events_to_redshift >> load_songplays_table
create_tables >> stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator