import airflow
from datetime import timedelta,datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
import json
import time
import requests
from datetime import datetime, timedelta
from airflow.models import DAG


default_args = {
    'owner': 'Meet1',
    'depends_on_past': False,
    'start_date': datetime(2022, 10, 30) ,
    'email': ['meetkothari@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    # 'retry_delay': timedelta(seconds=7),
    'schedule_interval': None,
    # 'provide_context': True
}
dag_psql = DAG(
    dag_id = "rest",
    default_args=default_args,
    # schedule_interval='0 0 * * *',
    description='use case of psql operator in airflow',
)

create_table_sql_query = """ 
CREATE TABLE employee (id INT NOT NULL, name VARCHAR(250) NOT NULL, dept VARCHAR(250) NOT NULL);
"""
insert_data_sql_query = """
    insert into employee (id, name, dept) 
    values(1, 'vamshi','bigdata'),(2, 'divya','bigdata'),
    (3, 'binny','projectmanager'),(4, 'omair','projectmanager'); """

create_table = PostgresOperator(
    sql = create_table_sql_query,
    task_id = "create_table_task",
    postgres_conn_id = "PostgreSQL_connect_hooks",
    dag = dag_psql
)

insert_data = PostgresOperator(
    sql = insert_data_sql_query,
    task_id = "insert_data_task",
    postgres_conn_id = "PostgreSQL_connect_hooks",
    dag = dag_psql
)
create_table >> insert_data
