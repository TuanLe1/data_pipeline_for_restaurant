import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


# 👇 1. THÊM ĐOẠN NÀY ĐỂ AIRFLOW TÌM THẤY FOLDER 'repos'
dag_folder = os.path.dirname(__file__)
repos_path = os.path.join(dag_folder, 'repos')
if repos_path not in sys.path:
    sys.path.append(repos_path)

# 👇 2. GIỜ MỚI IMPORT ĐƯỢC
try:
    from src.utils.slack_alert import task_fail_slack_alert
except ImportError:
    # Fallback phòng hờ
    from repos.src.utils.slack_alert import task_fail_slack_alert

# --- CẤU HÌNH ĐƯỜNG DẪN ---
AIRFLOW_INTERNAL_PATH = "/opt/airflow/dags/repos"
HOST_DBT_PATH = "/home/tuanle/DE-lab/data_pipeline_for_restaurant/dags/repos/dbt_project"

# Đường dẫn đến file script điều phối mới (bạn nhớ tạo file này như bước trước nhé)
ETL_SCRIPT_PATH = f"{AIRFLOW_INTERNAL_PATH}/scripts/run_etl.py"

default_args = {
    'owner': 'tuanle',
    'retries': 2, # Tăng lên 2 để nếu mạng lag thì tự thử lại
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_fail_slack_alert
}

with DAG(
    'restaurant_elt_pipeline',
    default_args=default_args,
    description='Full ELT: Extract -> Snowflake -> dbt',
    schedule_interval='0 8 * * *',
    start_date=datetime(2024, 1, 20),
    params={"manual_trigger": "yes"},
    max_active_runs=1,
    # max_active_tasks=1, # 👈 COMMENT DÒNG NÀY ĐỂ EXTRACT SONG SONG
    catchup=False,
    tags=['production', 'optimized'],
) as dag:

    # =================================================================
    # GROUP 1: MASTER DATA (Product, Customer)
    # =================================================================
    
    # 1.1. Extract (API -> Disk)
    t_master_extract = BashOperator(
        task_id='master_extract',
        bash_command=f'python3 {ETL_SCRIPT_PATH} --phase master --step extract'
    )

    # 1.2. Load (Disk -> S3)
    t_master_load = BashOperator(
        task_id='master_load',
        bash_command=f'python3 {ETL_SCRIPT_PATH} --phase master --step load'
    )

    # =================================================================
    # GROUP 2: TRANSACTION DATA (Orders, Invoices)
    # =================================================================

    # 2.1. Extract (API -> Disk)
    # {{ ds }} sẽ được Airflow thay thế bằng ngày chạy (YYYY-MM-DD)
    t_trans_extract = BashOperator(
        task_id='trans_extract',
        bash_command=f'python3 {ETL_SCRIPT_PATH} --phase trans --step extract --date ' + '{{ ds }}'
    )

    # 2.2. Load (Disk -> S3)
    t_trans_load = BashOperator(
        task_id='trans_load',
        bash_command=f'python3 {ETL_SCRIPT_PATH} --phase trans --step load --date ' + '{{ ds }}'
    )

    # =================================================================
    # GROUP 3: TRANSFORM (dbt)
    # =================================================================
    
    t_dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command=(
        f'cd {AIRFLOW_INTERNAL_PATH}/dbt_project && '
        'dbt build '
        '--profiles-dir . '
        '--project-dir .'
    )
)

    # =================================================================
    # 🔗 THIẾT LẬP DEPENDENCIES (LUỒNG CHẠY)
    # =================================================================
    
    # 1. Quy tắc nội bộ từng nhóm (Extract xong mới được Load)
    t_master_extract >> t_master_load
    t_trans_extract >> t_trans_load
    
    # 2. Quy tắc toàn cục (Load xong hết mới được chạy dbt)
    [t_master_load, t_trans_load] >> t_dbt_run