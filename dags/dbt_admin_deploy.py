import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Đường dẫn dự án dbt trong container
DBT_PROJECT_PATH = "/opt/airflow/dags/repos/dbt_project"

default_args = {
    'owner': 'tuanle',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'admin_dbt_deploy',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None, # Chỉ chạy manual khi Tuan muốn update logic
    catchup=False,
    tags=['admin', 'dbt'],
    description='DAG phục vụ cập nhật Logic Transform và khởi tạo bảng ClickHouse'
) as dag:

    # Task này đóng vai trò "xây nhà máy"
    dbt_run = BashOperator(
        task_id='deploy_new_transform_logic',
        bash_command=f'cd {DBT_PROJECT_PATH} && dbt run --profiles-dir .'
    )