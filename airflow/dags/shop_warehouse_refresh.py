import os
import logging
from datetime import datetime, timedelta
import requests
from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator

log = logging.getLogger(__name__)

DBT = "/home/airflow/dbt_venv/bin/dbt"
DBT_DIR = "/opt/airflow/dbt/shop_dw"


def notify_discord(context):
    url = os.environ.get("DISCORD_WEBHOOK_URL")
    if not url:
        log.warning("DISCORD_WEBHOOK_URL is not set, skipping alert")
        return
    ti = context["task_instance"]
    msg = f"`{ti.dag_id}.{ti.task_id}` failed (run `{context['run_id']}`)"
    resp = requests.post(url, json={"content": msg}, timeout=10)
    resp.raise_for_status()


default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": notify_discord,
}


@dag(
    dag_id="shop_warehouse_refresh",
    schedule="*/10 * * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["cdc", "dbt"],
)
def shop_warehouse_refresh():
    freshness = BashOperator(
        task_id="source_freshness",
        bash_command=f"cd {DBT_DIR} && {DBT} source freshness --profiles-dir .",
        retries=0,
    )
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && {DBT} run --profiles-dir .",
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && {DBT} test --profiles-dir .",
    )
    reconcile = BashOperator(
        task_id="reconcile",
        bash_command="python /opt/airflow/checks/reconcile.py --lag-minutes 15",
    )

    dbt_run >> dbt_test >> reconcile


shop_warehouse_refresh()