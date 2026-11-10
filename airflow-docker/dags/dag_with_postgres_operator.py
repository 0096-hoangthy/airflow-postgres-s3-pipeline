from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    'owner': 'ram',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='dag_with_postgres_operator_v03',
    default_args=default_args,
    start_date=datetime(2026, 11, 3),
    schedule='@daily',
    catchup=False
) as dag:

    task1 = SQLExecuteQueryOperator(
        task_id='create_postgres_table',
        conn_id='postgres_localhost',
        hook_params={"schema": "test"},  
        sql="""
            create table if not exists dag_runs (
                dt date,
                dag_id character varying,
                primary key (dt, dag_id)
            )
        """
    )

    task2 = SQLExecuteQueryOperator(
        task_id='insert_into_table',
        conn_id='postgres_localhost',  
        hook_params={"schema": "test"}, 
        sql="""
            insert into dag_runs (dt, dag_id) values ('{{ ds }}', '{{ dag.dag_id }}')
        """
    )

    task3 = SQLExecuteQueryOperator(
        task_id='delete_data_from_table',
        conn_id='postgres_localhost',  
        hook_params={"schema": "test"}, 
        sql="""
            delete from dag_runs where dt = '{{ ds }}'and dag_id = '{{ dag.dag_id }}'
        """
    )


    task1 >> task3 >> task2