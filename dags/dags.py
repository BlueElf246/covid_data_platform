from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='load_data_DWH_v1',
    default_args=default_args,
    description='DAG chạy từ 2019-12-31 đến 2020-01-20',
    schedule='0 0 * * *',
    start_date=datetime(2019, 12, 31),
    catchup=False,  # bật True nếu muốn backfill
    tags=['example', 'backfill'],
)
def data_quality_check_dag_v2():

    @task(task_id="test_connection")
    def run_connection():
        return BashOperator(
            task_id="test_connection_op",
            bash_command="python3 /u01/datle/project_root/ingestion/connection.py",
        ).execute({})

    @task(task_id="load_landing")
    def run_load_landing():
        return BashOperator(
            task_id="load_landing_op",
            bash_command="python3 /u01/datle/project_root/ingestion/load_landing.py",
        ).execute({})

    @task(task_id="load_to_silver")
    def run_transformation_1():
        return BashOperator(
            task_id="load_to_silver_op",
            bash_command="python3 /u01/datle/project_root/transformation/load_to_silver.py",
        ).execute({})

    @task(task_id="transform_data")
    def run_transformation_2():
        return BashOperator(
            task_id="transform_data_op",
            bash_command="python3 /u01/datle/project_root/transformation/transform_data.py",
        ).execute({})

    @task(task_id="checking_data_quality")
    def run_data_quality_check():
        return BashOperator(
            task_id="checking_data_quality_op",
            bash_command="python3 /u01/datle/project_root/data_quality/data_quality_check.py",
        ).execute({})

    @task(task_id="load_DWH")
    def run_load():
        return BashOperator(
            task_id="load_DWH_op",
            bash_command="python3 /u01/datle/project_root/load/load_to_dwh.py",
        ).execute({})

    # Chuỗi thực thi
    run_connection() >> run_load_landing() >> run_transformation_1() >> run_transformation_2() >> run_data_quality_check() >> run_load()


data_quality_check_dag_v2()
