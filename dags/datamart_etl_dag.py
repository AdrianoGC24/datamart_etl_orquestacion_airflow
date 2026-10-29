import os
import sys
from datetime import datetime, timedelta

# Ensure /opt/airflow/dags is in Python path for package resolution
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from scripts.extract import extract_daily_sales, extract_historical_sales
from scripts.transform import transform_and_validate
from scripts.load import load_star_schema_to_dw

default_args = {
    "owner": "datamart_data_engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="datamart_sales_etl_pipeline",
    default_args=default_args,
    description="Pipeline ETL diario idempotente hacia el Data Warehouse de DataMart S.A.S.",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["datamart", "etl", "star_schema"],
) as dag:

    # Lectura de Airflow Variables (con valores por defecto seguros)
    data_sources_dir = Variable.get("data_sources_dir", default_var="/opt/airflow/data_sources")
    staging_dir = Variable.get("staging_dir", default_var="/opt/airflow/logs/staging")
    use_catalog_api = Variable.get("use_catalog_api", default_var="false")

    # 1. Extracción de transacciones diarias (data.csv)
    extract_daily_task = PythonOperator(
        task_id="extract_daily_sales_csv",
        python_callable=extract_daily_sales,
        op_kwargs={
            "data_sources_dir": data_sources_dir,
            "staging_dir": staging_dir,
        },
    )

    # 2. Extracción del historial extendido (online_retail.csv)
    extract_historical_task = PythonOperator(
        task_id="extract_historical_sales_csv",
        python_callable=extract_historical_sales,
        op_kwargs={
            "data_sources_dir": data_sources_dir,
            "staging_dir": staging_dir,
        },
    )

    # 3. Transformación, limpieza, auditoría de rechazos y modelado dimensional
    transform_validate_task = PythonOperator(
        task_id="transform_validate_and_build_star_schema",
        python_callable=transform_and_validate,
        op_kwargs={
            "staging_dir": staging_dir,
            "use_catalog_api": use_catalog_api,
            "api_url": "http://fastapi-catalog:8000",
        },
    )

    # 4. Carga idempotente en PostgreSQL Data Warehouse usando la Connection 'postgres_dw'
    load_warehouse_task = PythonOperator(
        task_id="load_star_schema_to_postgres_dw",
        python_callable=load_star_schema_to_dw,
        op_kwargs={
            "staging_dir": staging_dir,
            "conn_id": "postgres_dw",
        },
    )

    # Dependencias explícitas: Extracción en paralelo -> Transformación -> Carga
    [extract_daily_task, extract_historical_task] >> transform_validate_task >> load_warehouse_task