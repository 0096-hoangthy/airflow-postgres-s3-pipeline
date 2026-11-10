from datetime import datetime, timedelta
import csv
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

default_args = {
    'owner': 'ram',
    'retries': 5,
    'retry_delay': timedelta(minutes=10)
}

def postgres_to_s3(**context):
    # Lấy ds và next_ds an toàn từ context của Airflow (nếu không có thì lấy ngày hiện tại)
    logical_date = context.get('logical_date') or datetime.now()
    ds_nodash = logical_date.strftime("%Y%m%d")
    
    # Tính ngày tiếp theo (next_ds)
    next_date = logical_date + timedelta(days=1)
    next_ds_nodash = next_date.strftime("%Y%m%d")
    
    # Format dạng chuẩn để câu lệnh SQL so sánh kiểu date
    ds_str = logical_date.strftime("%Y-%m-%d")
    next_ds_str = next_date.strftime("%Y-%m-%d")

    # Query dữ liệu từ PostgreSQL db và lưu vào file text
    hook = PostgresHook(postgres_conn_id="postgres_localhost")
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    cursor.execute("select * from orders where date >= %s and date < %s", (ds_str, next_ds_str))
    
    file_path = f"dags/get_orders_{ds_nodash}.txt"
    with open(file_path, "w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerows(cursor)
        
    cursor.close()
    conn.close()
    logging.info(f"Saved orders data in text file: {file_path}")

    #step2
    s3_hook = S3Hook(aws_conn_id="minio_conn")
    s3_hook.load_file(
        filename=file_path,
        key=f"orders/{ds_nodash}.txt",
        bucket_name="airflow",
        replace=True
    )


with DAG(
    dag_id="dag_with_postgres_hook_v03",
    default_args=default_args,
    start_date=datetime(2022, 4, 30),
    schedule='@daily'
) as dag:
    
    task1 = PythonOperator(
        task_id="postgres_to_s3",
        python_callable=postgres_to_s3,
        # Truyền thẳng context vào hàm bằng provide_context hoặc dùng **context mặc định
    )

    task1