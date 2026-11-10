from datetime import datetime, timedelta

from airflow import DAG 

from airflow.operators.bash import BashOperator

default_args = {
    'owner' : 'ram',
    'retries' : 5,
    'retry_delay': timedelta(minutes=2)
}



with DAG(
    dag_id = 'our_first_dag_v5',
    default_args= default_args,
    description="This is our first dag that we write",
    start_date=datetime(2026, 10, 24),
    schedule="@daily"

) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command='echo hello world, this is my first task'
    )

    task2 = BashOperator(
        task_id='second_task',
        bash_command='echo hey, i am task 2, running after task 1'
    )

    task3 = BashOperator (
        task_id = 'third_task',
        bash_command = "echo i will running after task1 sametime w task2"
    )

    #task1.set_downstream(task2)
    #task1.set_downstream(task3)

    
    #task1 >> task2
    #task2 >> task3

    task1 >> [task2, task3]