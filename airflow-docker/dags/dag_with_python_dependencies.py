from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'ram',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def get_sklearn():
    import sklearn
    print(f"Scikit-learn with version:{sklearn.__version__}")

def get_matplotlib():
    import matplotlib 
    print(f"matplotlib with the version: {matplotlib.__version__}")
with DAG(
    dag_id='dag_with_python_dependencies_v02',
    default_args=default_args,
    start_date=datetime(2026, 11, 4),
    schedule='@daily',
) as dag:


    get_sklearn = PythonOperator(
        task_id='get_sklearn',
        python_callable=get_sklearn
    )
    
    get_matplotlib = PythonOperator(
        task_id='get_matplotlib',
        python_callable=get_matplotlib
    )

    get_sklearn >> get_matplotlib