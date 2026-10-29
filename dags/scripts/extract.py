import os
import logging
import pandas as pd

logger = logging.getLogger(__name__)

# Columnas estándar que esperamos de ambas fuentes
STANDARD_COLUMNS = [
    "InvoiceNo", "StockCode", "Description", "Quantity",
    "InvoiceDate", "UnitPrice", "CustomerID", "Country"
]

# Mapeo defensivo por si el historial extendido trae nombres ligeramente distintos
COLUMN_MAPPING = {
    "Invoice": "InvoiceNo",
    "Price": "UnitPrice",
    "Customer ID": "CustomerID",
    "Customer_ID": "CustomerID"
}


def _read_and_standardize_csv(file_path: str, source_name: str) -> pd.DataFrame:
    """Lee un CSV crudo, elimina columnas índice basura y estandariza nombres."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"No se encontró el archivo fuente: {file_path}")

    logger.info(f"[{source_name}] Iniciando lectura desde {file_path}...")

    # Leemos todo como string (dtype=str) para preservar datos crudos intactos para auditoría
    try:
        df = pd.read_csv(file_path, encoding="utf-8", dtype=str, low_memory=False)
    except UnicodeDecodeError:
        logger.warning(f"[{source_name}] Falló UTF-8, aplicando fallback a latin1...")
        df = pd.read_csv(file_path, encoding="latin1", dtype=str, low_memory=False)

    # 1. Eliminar columna 'index' o 'Unnamed: 0' presente en online_retail.csv
    cols_to_drop = [c for c in df.columns if c.lower() in ("index", "unnamed: 0")]
    if cols_to_drop:
        df = df.drop(columns=cols_to_drop)

    # 2. Renombrar columnas si vienen con nombres alternativos
    df = df.rename(columns=COLUMN_MAPPING)

    # 3. Asegurar que estén las 8 columnas base
    missing_cols = set(STANDARD_COLUMNS) - set(df.columns)
    if missing_cols:
        raise ValueError(f"[{source_name}] Faltan columnas obligatorias: {missing_cols}")

    df = df[STANDARD_COLUMNS].copy()
    df["source_file"] = source_name

    logger.info(f"[{source_name}] Extraídas {len(df):,} filas exitosamente.")
    return df


def extract_daily_sales(data_sources_dir: str, staging_dir: str) -> str:
    """Tarea 1: Extrae data.csv (ventas diarias) y guarda en staging parquet."""
    os.makedirs(staging_dir, exist_ok=True)
    input_path = os.path.join(data_sources_dir, "data.csv")
    output_path = os.path.join(staging_dir, "raw_daily.parquet")

    df = _read_and_standardize_csv(input_path, source_name="data.csv")
    df.to_parquet(output_path, index=False)
    return output_path


def extract_historical_sales(data_sources_dir: str, staging_dir: str) -> str:
    """Tarea 2: Extrae online_retail.csv (historial extendido) y guarda en staging parquet."""
    os.makedirs(staging_dir, exist_ok=True)
    # Soporta tanto online_retail.csv como online_retail_II.csv
    input_path = os.path.join(data_sources_dir, "online_retail.csv")
    if not os.path.exists(input_path):
        input_path = os.path.join(data_sources_dir, "online_retail_II.csv")

    output_path = os.path.join(staging_dir, "raw_historical.parquet")

    df = _read_and_standardize_csv(input_path, source_name="online_retail.csv")
    df.to_parquet(output_path, index=False)
    return output_path