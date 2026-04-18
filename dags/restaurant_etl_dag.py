import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models.param import Param # Import để tạo UI chọn ngày

# 1. SETUP ĐƯỜNG DẪN
AIRFLOW_INTERNAL_PATH = "/opt/airflow/dags/repos"
ETL_SCRIPT_PATH = f"{AIRFLOW_INTERNAL_PATH}/scripts/run_etl.py"
DBT_PROJECT_PATH = f"{AIRFLOW_INTERNAL_PATH}/dbt_project"

dag_folder = os.path.dirname(__file__)
repos_path = os.path.join(dag_folder, 'repos')
if repos_path not in sys.path:
    sys.path.append(repos_path)

try:
    from src.utils.slack_alert import task_fail_slack_alert
except ImportError:
    task_fail_slack_alert = None

default_args = {
    'owner': 'tuanle',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_fail_slack_alert
}

with DAG(
    'restaurant_elt_pipeline',
    default_args=default_args,
    description='Full Sync Master & Transactions with Manual Date Selection',
    schedule_interval='0 1 * * *', 
    start_date=datetime(2024, 1, 20),
    catchup=False,
    max_active_runs=1,
    tags=['production', 'parameterized'],
    # Thêm cấu hình tham số ở đây
    params={
        "target_date": Param(
            default=None, 
            type=["string", "null"], 
            format="date",
            description="Chọn ngày chạy (YYYY-MM-DD). Để trống để lấy ngày chạy tự động (ds)."
        )
    },
) as dag:

    # Logic Jinja: Ưu tiên lấy ngày từ params, nếu không có thì lấy {{ ds }} (ngày của lịch chạy)
    # Chúng ta dùng một biến trung gian để câu lệnh bash sạch sẽ hơn
    TARGET_DATE = "{{ params.target_date if params.target_date else ds }}"

    # --- NHÁNH 1: MASTER DATA ---
    t_master_extract = BashOperator(
        task_id='master_extract_to_s3',
        bash_command=f'python3 {ETL_SCRIPT_PATH} --phase master --step extract'
    )

    t_master_load = BashOperator(
        task_id='master_load_to_clickhouse',
        bash_command=f'python3 {ETL_SCRIPT_PATH} --phase master --step load'
    )

    # --- NHÁNH 2: TRANSACTION DATA ---
    t_trans_sync = BashOperator(
        task_id='transactions_sync_to_s3',
        # Sử dụng biến TARGET_DATE đã khai báo ở trên
        bash_command=f'python3 {ETL_SCRIPT_PATH} --phase trans --step extract --date {TARGET_DATE}'
    )

    # --- CUỐI CÙNG: DBT TRANSFORM ---
    t_dbt_run = BashOperator(
        task_id='dbt_transform_and_test',
        bash_command=(
            f'cd {DBT_PROJECT_PATH} && '
            f'dbt run --profiles-dir . && '   # bỏ --full-refresh ở đây
            f'dbt test --profiles-dir .'      # chạy tất cả tests sau mỗi run
        )
    )

    # THIẾT LẬP LUỒNG CHẠY
    t_master_extract >> t_master_load
    [t_master_load, t_trans_sync] >> t_dbt_run