FROM apache/airflow:3.1.0

RUN python -m venv /home/airflow/dbt_venv \
 && /home/airflow/dbt_venv/bin/pip install --no-cache-dir \
      "dbt-core==1.10.23" "dbt-postgres==1.9.1"

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" psycopg2-binary pandas