import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Scripts
SCRIPT_EXTRACT = "/opt/airflow/scripts/extract_data.py"
SCRIPT_LOAD = "/opt/airflow/scripts/load_data.py"
PYTHONPATH = "/opt/airflow:/opt/airflow/scripts:/opt/airflow/config"

DATA_DIR = "/opt/airflow/data/extracted"

os.makedirs(DATA_DIR, exist_ok=True)

with DAG(
    dag_id="elt_dag",
    start_date=datetime(2025, 10, 23),
    schedule_interval="0 3 * * *",
    catchup=False,
    tags=["extract", "load", "transform", "portfolio"]
) as dag:

    extract_task = BashOperator(
        task_id="extract_data",
        bash_command=f"PYTHONPATH={PYTHONPATH} python {SCRIPT_EXTRACT} --data-dir {DATA_DIR}"
    )

    load_task = BashOperator(
        task_id="load_data",
        bash_command=f"PYTHONPATH={PYTHONPATH} python {SCRIPT_LOAD}"
    )

    dbt_staging_task = BashOperator(
        task_id="dbt_run_staging",
        bash_command="cd /opt/airflow/dbt && dbt run --select staging"
    )

    dbt_seed_task = BashOperator(
        task_id="dbt_seed",
        bash_command="cd /opt/airflow/dbt && dbt seed"
    )

    dbt_core_task = BashOperator(
        task_id="dbt_run_core",
        bash_command="cd /opt/airflow/dbt && dbt run --select core"
    )

    dbt_test_task = BashOperator(
        task_id="dbt_test_core",
        bash_command="cd /opt/airflow/dbt && dbt test --select core --store-failures"
    )

    dbt_marts_task = BashOperator(
        task_id="dbt_run_marts",
        bash_command="cd /opt/airflow/dbt && dbt run --select marts"
    )


    extract_task >> load_task >> dbt_staging_task >> dbt_seed_task >> dbt_core_task >> dbt_test_task >> dbt_marts_task