from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'docker_etl_dag',
    default_args=default_args,
    description='DAG for running ETL Docker container',
    schedule_interval=timedelta(days=1),
)

with dag:
    etl_task = DockerOperator(
        task_id='docker_etl_task',
        image='yourdockerhubusername/your_etl_image:tag',
        api_version='auto',
        auto_remove=True,
        command="/bin/sleep 30",  # You can override CMD defined in Dockerfile if needed
        docker_url="unix://var/run/docker.sock",  # Assumes Docker is running on the same host as Airflow
        network_mode="bridge"
    )

    etl_task