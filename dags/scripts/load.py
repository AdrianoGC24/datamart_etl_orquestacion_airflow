import io
import os
import logging
import pandas as pd
from sqlalchemy import create_engine
from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)


def _bulk_upsert_via_copy(conn, df: pd.DataFrame, target_table: str, conflict_cols: list, update_cols: list = None):
    """
    Realiza una carga masiva ultrarrápida e idempotente en PostgreSQL:
    1. Crea una tabla temporal en la sesión.
    2. Usa COPY FROM STDIN (en memoria con StringIO) para subir miles de filas por segundo.
    3. Ejecuta INSERT INTO ... SELECT ... ON CONFLICT para garantizar idempotencia total.
    """
    if df.empty:
        logger.info(f"[{target_table}] DataFrame vacío, se omite carga.")
        return

    temp_table = f"tmp_{target_table}"
    columns = list(df.columns)
    cols_sql = ", ".join(columns)
    conflict_sql = ", ".join(conflict_cols)

    with conn.cursor() as cur:
        # 1. Crear tabla temporal idéntica a las columnas a insertar
        cur.execute(f"CREATE TEMP TABLE {temp_table} (LIKE {target_table} INCLUDING DEFAULTS) ON COMMIT DROP;")

        # 2. Volcar DataFrame a buffer en memoria y ejecutar COPY
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False, sep="\t", na_rep="\\N")
        buffer.seek(0)
        cur.copy_expert(f"COPY {temp_table} ({cols_sql}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')", buffer)

        # 3. Upsert idempotente hacia la tabla final
        if update_cols:
            update_stmt = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_cols])
            on_conflict_clause = f"ON CONFLICT ({conflict_sql}) DO UPDATE SET {update_stmt}"
        else:
            on_conflict_clause = f"ON CONFLICT ({conflict_sql}) DO NOTHING"

        upsert_query = f"""
            INSERT INTO {target_table} ({cols_sql})
            SELECT {cols_sql} FROM {temp_table}
            {on_conflict_clause};
        """
        cur.execute(upsert_query)
        logger.info(f"[{target_table}] Procesadas {len(df):,} filas de forma idempotente.")


def load_star_schema_to_dw(staging_dir: str, conn_id: str = "postgres_dw"):
    """
    Tarea 4: Conecta al Data Warehouse mediante Airflow Connection y carga las 6 tablas del Modelo Estrella.
    """
    # Obtener la Airflow Connection configurada en el entorno
    airflow_conn = BaseHook.get_connection(conn_id)
    uri = airflow_conn.get_uri()
    if uri.startswith("postgres://"):
        uri = uri.replace("postgres://", "postgresql+psycopg2://", 1)
    elif uri.startswith("postgresql://"):
        uri = uri.replace("postgresql://", "postgresql+psycopg2://", 1)

    engine = create_engine(uri)

    # Leer tablas transformadas desde staging
    dim_date = pd.read_parquet(os.path.join(staging_dir, "dim_date.parquet"))
    dim_products = pd.read_parquet(os.path.join(staging_dir, "dim_products.parquet"))
    dim_customers = pd.read_parquet(os.path.join(staging_dir, "dim_customers.parquet"))
    fact_transactions = pd.read_parquet(os.path.join(staging_dir, "fact_transactions.parquet"))
    fact_daily_metrics = pd.read_parquet(os.path.join(staging_dir, "fact_daily_product_metrics.parquet"))
    audit_rejected = pd.read_parquet(os.path.join(staging_dir, "audit_rejected_records.parquet"))

    # Ejecutar toda la carga dentro de una transacción atómica
    raw_conn = engine.raw_connection()
    try:
        # 1. Dimensiones
        _bulk_upsert_via_copy(
            raw_conn, dim_date, "dim_date",
            conflict_cols=["date_key"]
        )
        _bulk_upsert_via_copy(
            raw_conn, dim_products, "dim_products",
            conflict_cols=["stock_code"],
            update_cols=["canonical_description", "category", "raw_variation_count"]
        )
        _bulk_upsert_via_copy(
            raw_conn, dim_customers, "dim_customers",
            conflict_cols=["customer_id"],
            update_cols=["is_identified", "primary_country"]
        )

        # 2. Tablas de Hechos
        _bulk_upsert_via_copy(
            raw_conn, fact_transactions, "fact_transactions",
            conflict_cols=["transaction_hash"]
        )
        _bulk_upsert_via_copy(
            raw_conn, fact_daily_metrics, "fact_daily_product_metrics",
            conflict_cols=["date_key", "stock_code"],
            update_cols=["sales_count", "returns_count", "units_sold", "units_returned", "gross_revenue", "returns_amount", "net_revenue"]
        )

        # 3. Tabla de Auditoría de Rechazos
        _bulk_upsert_via_copy(
            raw_conn, audit_rejected, "audit_rejected_records",
            conflict_cols=["reject_hash"]
        )

        raw_conn.commit()
        logger.info("¡Carga completa e idempotente en el Data Warehouse finalizada con éxito!")
    except Exception as e:
        raw_conn.rollback()
        logger.error(f"Error durante la carga al Data Warehouse. Se aplicó ROLLBACK: {e}")
        raise
    finally:
        raw_conn.close()