import datetime
import logging

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

##### ------ Define python function -------
def load_data_to_redshift(*args, **kwargs):
    """
    Function to load data from s3 to redshift using the copy command
    """
    aws_hook = AwsHook("aws_credentials") #from Step 2.2.1. Set up credentials
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift") #from Step 2.2.2. Set up connection to redshift
    ## print info to make sure we get the right access_key and secret_key, since
    # it often gets wrong during copy/paste
    logging.info(sql_statements.COPY_ALL_TRIPS_SQL.format(credentials.access_key, credentials.secret_key))
    ## Run the copy command
    redshift_hook.run(sql_statements.COPY_ALL_TRIPS_SQL.format(credentials.access_key, credentials.secret_key))

##### ------ Define SQL statements -------

CREATE_TRIPS_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS trips (
    trip_id INTEGER NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    bikeid INTEGER NOT NULL,
    tripduration DECIMAL(16,2) NOT NULL,
    from_station_id INTEGER NOT NULL,
    from_station_name VARCHAR(100) NOT NULL,
    to_station_id INTEGER NOT NULL,
    to_station_name VARCHAR(100) NOT NULL,
    usertype VARCHAR(20),
    gender VARCHAR(6),
    birthyear INTEGER,
    PRIMARY KEY(trip_id))
    DISTSTYLE ALL;
    """

COPY_SQL = """
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{{}}'
    SECRET_ACCESS_KEY '{{}}'
    IGNOREHEADER 1
    DELIMITER ','
    """

COPY_ALL_TRIPS_SQL = COPY_SQL.format(
    "trips",
    "s3://udacity-dend/data-pipelines/divvy/unpartitioned/divvy_trips_2018.csv"
)

STATION_TRAFFIC_SQL = """
    DROP TABLE IF EXISTS station_traffic;
    CREATE TABLE station_traffic AS
    SELECT
        DISTINCT(t.from_station_id) AS station_id,
        t.from_station_name AS station_name,
        num_departures,
        num_arrivals
    FROM trips t
    JOIN (
        SELECT
            from_station_id,
            COUNT(from_station_id) AS num_departures
        FROM trips
        GROUP BY from_station_id
    ) AS s1 ON t.from_station_id = s1.from_station_id
    JOIN (
        SELECT
            to_station_id,
            COUNT(to_station_id) AS num_arrivals
        FROM trips
        GROUP BY to_station_id
    ) AS s2 ON t.from_station_id = s2.to_station_id
    """

##### ------ Define dag -------
dag = DAG(
    'data_pipeline_airflow_Redshift',
    start_date = datetime.datetime.now()
)

##### ------ Define tasks -------
### --- Task with PostgresOperator ---
# Create table
create_table = PostgresOperator(
    task_id = "create_table",
    dag = dag,
    postgres_conn_id = "redshift",
    sql = sql_statements.CREATE_TRIPS_TABLE_SQL
)
# Traffic analysis
location_traffic_task = PostgresOperator(
    task_id = "calculate_location_traffic",
    dag = dag,
    postgres_conn_id = "redshift",
    sql = sql_statements.STATION_TRAFFIC_SQL
)

### --- Task with PythonOperator ---
copy_task = PythonOperator(
    task_id = 'load_from_s3_to_redshift',
    dag = dag,
    python_callable = load_data_to_redshift
)


##### ------ Configure the task dependencies -------
# Task dependencies such that the graph looks like the following:
# create_table -> copy_task -> location_traffic_task

create_table >> copy_task
copy_task >> location_traffic_task