import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# --- CẤU HÌNH ĐƯỜNG DẪN (Copy từ DAG chính sang) ---
HOST_DBT_PATH = "/home/tuanle/DE-lab/data_pipeline_for_restaurant/dags/repos/dbt_project"
AIRFLOW_INTERNAL_PATH = "/opt/airflow/dags/repos"
ETL_SCRIPT_PATH = f"{AIRFLOW_INTERNAL_PATH}/scripts/run_etl.py"

# --- MẶC ĐỊNH CONFIG ---
# Khi chạy tự động (theo lịch), nó sẽ lấy giá trị này
default_conf = {
    "lookback_days": 7  # Mặc định quét lại 7 ngày gần nhất
}

default_args = {
    'owner': 'tuanle',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'maintenance_weekly_backfill',
    default_args=default_args,
    description='Chạy lại dữ liệu quá khứ để đảm bảo tính toàn vẹn (Self-healing)',
    schedule_interval='0 3 * * 0', 
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['maintenance', 'backfill'],
    params=default_conf,
) as dag:

    # 1. Task Backfill (Giữ nguyên logic của bạn, rất tốt!)
    t_backfill_loop = BashOperator(
        task_id='backfill_extract_load',
        bash_command=f"""
        DAYS={{{{ params.lookback_days }}}}
        echo "🔄 Bắt đầu Backfill cho $DAYS ngày gần nhất..."
        
        for i in $(seq $DAYS -1 1)
        do
            TARGET_DATE=$(date -d "-$i days" +%Y-%m-%d)
            echo "------------------------------------------------"
            echo "🚀 Processing Date: $TARGET_DATE"
            
            # Extract & Load Transaction
            python3 {ETL_SCRIPT_PATH} --phase trans --step extract --date $TARGET_DATE
            python3 {ETL_SCRIPT_PATH} --phase trans --step load --date $TARGET_DATE
            
            echo "✅ Done $TARGET_DATE"
        done
        """
    )

    # 2. Chạy dbt với tham số dynamic
    t_dbt_run = BashOperator(
    task_id='dbt_incremental_heal',
    bash_command=(
        f'cd {HOST_DBT_PATH} && '
        'dbt build '
        '--profiles-dir . '
        '--project-dir . '
        '--vars \'{"backfill_days": {{ params.lookback_days }}}\''
    )
)

    t_backfill_loop >> t_dbt_run