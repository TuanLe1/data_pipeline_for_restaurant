from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime

# Đường dẫn project trên máy HOST (EC2/WSL)
# Bạn nhớ sửa đường dẫn này cho đúng nơi bạn lưu code dbt
HOST_PROJECT_PATH = "/home/tuanle/DE-lab/data_pipeline_for_restaurant"

with DAG('test_docker_operator', start_date=datetime(2024, 1, 1), schedule_interval=None) as dag:

    dbt_task = DockerOperator(
        task_id='run_dbt_container',
        # Image này sẽ được pull về máy host và chạy độc lập
        image='custom-dbt-athena:1.7.1',
        force_pull=False,
        api_version='auto',
        auto_remove=True,
        # Lệnh dbt run
        command="dbt run --profiles-dir /dbt_project --project-dir /dbt_project",
        mount_tmp_dir=False,
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mounts=[
            # Mount code dbt project vào container con
            Mount(source=f"{HOST_PROJECT_PATH}/dags/repos/dbt_project", target="/dbt_project", type="bind"),
            # Mount AWS Creds vào container con để nó connect Athena
            Mount(source="/home/tuanle/.aws", target="/root/.aws", type="bind"),
        ],
        environment={
            'AWS_REGION': 'ap-southeast-2'
        }
    )