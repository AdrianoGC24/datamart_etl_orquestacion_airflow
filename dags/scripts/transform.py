import os
import hashlib
import logging
import requests
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def _assign_categories_vectorized(descriptions: pd.Series) -> pd.Series:
    """
    Asigna una de las 5 categorías de DataMart S.A.S. (Electrónica, Hogar, Ropa, Deportes, Papelería)
    de forma vectorizada usando palabras clave sobre la descripción canónica.
    """
    desc_upper = descriptions.fillna("").str.upper()

    cond_electronica = desc_upper.str.contains(
        r"LIGHT|LAMP|CLOCK|RADIO|TORCH|USB|CAMERA|PHONE|CALCULATOR|BATTERY|LED|ALARM|PLUG|BULB",
        regex=True
    )
    cond_papeleria = desc_upper.str.contains(
        r"PAPER|PEN|PENCIL|NOTEBOOK|CARD|STICKER|BOOK|WRAP|TAPE|RIBBON|DIARY|ERASER|RULER|STAMP|ENVELOPE|POSTAGE",
        regex=True
    )
    cond_ropa = desc_upper.str.contains(
        r"BAG|SHIRT|DRESS|SOCK|HAT|SCARF|APRON|PURSE|WALLET|UMBRELLA|SLIPPER|COAT|GLOVE|JEWEL|NECKLACE|BRACELET|RING",
        regex=True
    )
    cond_deportes = desc_upper.str.contains(
        r"BALL|GAME|BIKE|CYCLE|SPORT|CAMPING|PICNIC|BOTTLE|SKIPPING|DART|KITE|PUZZLE|TOY|OUTDOOR|BEACH",
        regex=True
    )

    # Si no cae en las 4 anteriores, por naturaleza del dataset de decoración/bazar, pertenece a 'Hogar'
    return np.select(
        [cond_electronica, cond_papeleria, cond_ropa, cond_deportes],
        ["Electrónica", "Papelería", "Ropa", "Deportes"],
        default="Hogar"
    )


def _fetch_categories_from_api(stock_codes: list, api_url: str) -> dict:
    """Plus opcional (Sección 4.3): Consulta la API interna de catálogo si está activa."""
    catalog_map = {}
    try:
        response = requests.get(f"{api_url}/products", params={"limit": 10000}, timeout=10)
        if response.status_code == 200:
            items = response.json().get("items", [])
            for item in items:
                catalog_map[item["stock_code"]] = item
            logger.info(f"Catálogo enriquecido desde API con {len(catalog_map)} productos.")
    except Exception as e:
        logger.warning(f"No se pudo conectar a la API ({e}). Usando estrategia estática de respaldo.")
    return catalog_map


def transform_and_validate(staging_dir: str, use_catalog_api: str = "false", api_url: str = "") -> dict:
    """
    Tarea 3: Unifica fuentes, audita rechazos, resuelve casos ambiguos y genera las 6 tablas del Modelo Estrella.
    """
    logger.info("Cargando archivos Parquet desde staging...")
    df_daily = pd.read_parquet(os.path.join(staging_dir, "raw_daily.parquet"))
    df_hist = pd.read_parquet(os.path.join(staging_dir, "raw_historical.parquet"))

    # 1. Unificar dando prioridad a data.csv (aparece primero antes del drop_duplicates)
    df_all = pd.concat([df_daily, df_hist], ignore_index=True)
    total_raw = len(df_all)

    # Limpieza básica de strings para detectar duplicados reales entre ambos CSV
    df_all["InvoiceNo_clean"] = df_all["InvoiceNo"].astype(str).str.strip()
    df_all["StockCode_clean"] = df_all["StockCode"].astype(str).str.strip().str.upper().str.replace(" ", "", regex=False)
    df_all["CustomerID_clean"] = (
        df_all["CustomerID"]
        .fillna("UNIDENTIFIED")
        .astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
    )
    df_all.loc[df_all["CustomerID_clean"].isin(["", "nan", "None", "NaN"]), "CustomerID_clean"] = "UNIDENTIFIED"

    # Conversión numérica segura y de fechas a UTC
    df_all["Quantity_num"] = pd.to_numeric(df_all["Quantity"], errors="coerce")
    df_all["UnitPrice_num"] = pd.to_numeric(df_all["UnitPrice"], errors="coerce")
    df_all["InvoiceDate_utc"] = pd.to_datetime(df_all["InvoiceDate"], errors="coerce", utc=True)

    # 2. Resolver Caso Ambiguo 3: Eliminar duplicados exactos entre data.csv y online_retail.csv
    # Usamos las columnas de negocio + el número de fila dentro de la misma factura/producto/precio
    dedup_cols = ["InvoiceNo_clean", "StockCode_clean", "InvoiceDate", "Quantity_num", "UnitPrice_num", "CustomerID_clean"]
    df_all["occurrence_idx"] = df_all.groupby(dedup_cols, dropna=False).cumcount()
    df_dedup = df_all.drop_duplicates(subset=dedup_cols + ["occurrence_idx"], keep="first").copy()
    logger.info(f"Filas totales: {total_raw:,} | Tras deduplicar solapamiento entre fuentes: {len(df_dedup):,}")

    # =========================================================================
    # 3. AUDITORÍA Y SEPARACIÓN DE RECHAZOS (Sección 5 y 6)
    # =========================================================================
    cond_missing_critical = (
        df_dedup["InvoiceNo"].isna() |
        df_dedup["StockCode"].isna() |
        (df_dedup["StockCode_clean"] == "") |
        df_dedup["Quantity_num"].isna() |
        df_dedup["UnitPrice_num"].isna() |
        df_dedup["InvoiceDate_utc"].isna()
    )
    cond_invalid_price = (~cond_missing_critical) & (df_dedup["UnitPrice_num"] <= 0)
    cond_zero_qty = (~cond_missing_critical) & (~cond_invalid_price) & (df_dedup["Quantity_num"] == 0)

    is_rejected = cond_missing_critical | cond_invalid_price | cond_zero_qty

    df_rejected = df_dedup[is_rejected].copy()
    df_rejected["rejection_reason"] = np.select(
        [
            cond_missing_critical[is_rejected],
            cond_invalid_price[is_rejected],
            cond_zero_qty[is_rejected]
        ],
        [
            "MISSING_OR_CORRUPT_CRITICAL_FIELDS",
            "UNIT_PRICE_ZERO_OR_NEGATIVE",
            "QUANTITY_EQUAL_TO_ZERO"
        ],
        default="UNKNOWN_QUALITY_ERROR"
    )

    # Hash idempotente para la tabla de rechazos
    reject_key_series = (
        df_rejected["InvoiceNo"].fillna("") + "|" +
        df_rejected["StockCode"].fillna("") + "|" +
        df_rejected["Quantity"].fillna("") + "|" +
        df_rejected["UnitPrice"].fillna("") + "|" +
        df_rejected["InvoiceDate"].fillna("") + "|" +
        df_rejected["source_file"].fillna("") + "|" +
        df_rejected["occurrence_idx"].astype(str)
    )
    df_rejected["reject_hash"] = reject_key_series.apply(lambda x: hashlib.md5(x.encode("utf-8")).hexdigest())

    audit_table = df_rejected[[
        "reject_hash", "InvoiceNo", "StockCode", "Description", "Quantity",
        "UnitPrice", "InvoiceDate", "CustomerID", "Country", "rejection_reason", "source_file"
    ]].rename(columns={
        "InvoiceNo": "raw_invoice_no",
        "StockCode": "raw_stock_code",
        "Description": "raw_description",
        "Quantity": "raw_quantity",
        "UnitPrice": "raw_unit_price",
        "InvoiceDate": "raw_invoice_date",
        "CustomerID": "raw_customer_id",
        "Country": "raw_country"
    }).drop_duplicates(subset=["reject_hash"])

    # =========================================================================
    # 4. TRANSFORMACIÓN DE REGISTROS VÁLIDOS
    # =========================================================================
    df_valid = df_dedup[~is_rejected].copy()
    df_valid["Quantity_num"] = df_valid["Quantity_num"].astype(int)
    df_valid["Country_clean"] = df_valid["Country"].fillna("Unspecified").str.strip()
    df_valid["Description_clean"] = (
        df_valid["Description"]
        .fillna("NO DESCRIPTION")
        .astype(str)
        .str.strip()
        .str.upper()
        .str.replace(r"\s+", " ", regex=True)
    )

    # Regla de negocio: Quantity > 0 -> SALE, Quantity < 0 -> RETURN
    df_valid["transaction_type"] = np.where(df_valid["Quantity_num"] > 0, "SALE", "RETURN")
    df_valid["gross_amount"] = (df_valid["Quantity_num"] * df_valid["UnitPrice_num"]).round(4)
    df_valid["date_key"] = df_valid["InvoiceDate_utc"].dt.strftime("%Y%m%d").astype(int)

    # Hash determinístico para idempotencia en fact_transactions
    tx_key_series = (
        df_valid["InvoiceNo_clean"] + "|" +
        df_valid["StockCode_clean"] + "|" +
        df_valid["InvoiceDate_utc"].astype(str) + "|" +
        df_valid["Quantity_num"].astype(str) + "|" +
        df_valid["UnitPrice_num"].astype(str) + "|" +
        df_valid["CustomerID_clean"] + "|" +
        df_valid["occurrence_idx"].astype(str)
    )
    df_valid["transaction_hash"] = tx_key_series.apply(lambda x: hashlib.md5(x.encode("utf-8")).hexdigest())

    # =========================================================================
    # 5. CONSTRUCCIÓN DE DIMENSIONES Y TABLAS DE HECHOS
    # =========================================================================

    # A. dim_date
    unique_dates = pd.DataFrame({"full_date": df_valid["InvoiceDate_utc"].dt.date.unique()})
    unique_dates["full_date_dt"] = pd.to_datetime(unique_dates["full_date"])
    dim_date = pd.DataFrame({
        "date_key": unique_dates["full_date_dt"].dt.strftime("%Y%m%d").astype(int),
        "full_date": unique_dates["full_date"],
        "year": unique_dates["full_date_dt"].dt.year,
        "quarter": unique_dates["full_date_dt"].dt.quarter,
        "month": unique_dates["full_date_dt"].dt.month,
        "month_name": unique_dates["full_date_dt"].dt.strftime("%B"),
        "day": unique_dates["full_date_dt"].dt.day,
        "day_of_week": unique_dates["full_date_dt"].dt.dayofweek + 1,
        "is_weekend": (unique_dates["full_date_dt"].dt.dayofweek + 1).isin([6, 7])
    }).sort_values("date_key")

    # B. dim_products (Caso Ambiguo 2: Nombre canónico por moda y conteo de variaciones)
    raw_variations = (
        df_valid.groupby("StockCode_clean")["Description"]
        .nunique(dropna=True)
        .reset_index(name="raw_variation_count")
    )
    # Elegimos la descripción limpia más frecuente (moda) para cada StockCode
    desc_counts = (
        df_valid.groupby(["StockCode_clean", "Description_clean"])
        .size()
        .reset_index(name="cnt")
        .sort_values(["StockCode_clean", "cnt"], ascending=[True, False])
        .drop_duplicates(subset=["StockCode_clean"], keep="first")
    )
    dim_products = pd.merge(desc_counts[["StockCode_clean", "Description_clean"]], raw_variations, on="StockCode_clean")
    dim_products = dim_products.rename(columns={
        "StockCode_clean": "stock_code",
        "Description_clean": "canonical_description"
    })
    dim_products["category"] = _assign_categories_vectorized(dim_products["canonical_description"])
    dim_products["supplier_country"] = "United Kingdom"
    dim_products["is_active"] = True

    # Si el plus de la API está activo, enriquecemos desde la API
    if str(use_catalog_api).lower() == "true" and api_url:
        api_catalog = _fetch_categories_from_api(dim_products["stock_code"].tolist(), api_url)
        if api_catalog:
            dim_products["category"] = dim_products["stock_code"].map(
                lambda sc: api_catalog.get(sc, {}).get("category")
            ).fillna(dim_products["category"])

    # C. dim_customers (Caso Ambiguo 1: Clientes identificados vs UNIDENTIFIED)
    cust_country = (
        df_valid.groupby(["CustomerID_clean", "Country_clean"])
        .size()
        .reset_index(name="cnt")
        .sort_values(["CustomerID_clean", "cnt"], ascending=[True, False])
        .drop_duplicates(subset=["CustomerID_clean"], keep="first")
    )
    dim_customers = pd.DataFrame({
        "customer_id": cust_country["CustomerID_clean"],
        "is_identified": cust_country["CustomerID_clean"] != "UNIDENTIFIED",
        "primary_country": cust_country["Country_clean"]
    })

    # D. fact_transactions
    fact_transactions = df_valid[[
        "transaction_hash", "InvoiceNo_clean", "StockCode_clean", "CustomerID_clean",
        "date_key", "InvoiceDate_utc", "Country_clean", "transaction_type",
        "Quantity_num", "UnitPrice_num", "gross_amount", "source_file"
    ]].rename(columns={
        "InvoiceNo_clean": "invoice_no",
        "StockCode_clean": "stock_code",
        "CustomerID_clean": "customer_id",
        "InvoiceDate_utc": "invoice_timestamp_utc",
        "Country_clean": "country",
        "Quantity_num": "quantity",
        "UnitPrice_num": "unit_price"
    }).drop_duplicates(subset=["transaction_hash"])

    # E. fact_daily_product_metrics (Regla Sección 5: Revenue Bruto y Neto diario por producto)
    is_sale = fact_transactions["transaction_type"] == "SALE"
    fact_temp = fact_transactions.copy()
    fact_temp["sales_count"] = np.where(is_sale, 1, 0)
    fact_temp["returns_count"] = np.where(~is_sale, 1, 0)
    fact_temp["units_sold"] = np.where(is_sale, fact_temp["quantity"], 0)
    fact_temp["units_returned"] = np.where(~is_sale, fact_temp["quantity"].abs(), 0)
    fact_temp["gross_rev_item"] = np.where(is_sale, fact_temp["gross_amount"], 0.0)
    fact_temp["returns_amt_item"] = np.where(~is_sale, fact_temp["gross_amount"].abs(), 0.0)

    fact_daily_metrics = fact_temp.groupby(["date_key", "stock_code"], as_index=False).agg(
        sales_count=("sales_count", "sum"),
        returns_count=("returns_count", "sum"),
        units_sold=("units_sold", "sum"),
        units_returned=("units_returned", "sum"),
        gross_revenue=("gross_rev_item", "sum"),
        returns_amount=("returns_amt_item", "sum")
    )
    fact_daily_metrics["gross_revenue"] = fact_daily_metrics["gross_revenue"].round(4)
    fact_daily_metrics["returns_amount"] = fact_daily_metrics["returns_amount"].round(4)
    fact_daily_metrics["net_revenue"] = (
        fact_daily_metrics["gross_revenue"] - fact_daily_metrics["returns_amount"]
    ).round(4)

    # Guardar las 6 tablas procesadas en staging para la etapa de carga
    dim_date.to_parquet(os.path.join(staging_dir, "dim_date.parquet"), index=False)
    dim_products.to_parquet(os.path.join(staging_dir, "dim_products.parquet"), index=False)
    dim_customers.to_parquet(os.path.join(staging_dir, "dim_customers.parquet"), index=False)
    fact_transactions.to_parquet(os.path.join(staging_dir, "fact_transactions.parquet"), index=False)
    fact_daily_metrics.to_parquet(os.path.join(staging_dir, "fact_daily_product_metrics.parquet"), index=False)
    audit_table.to_parquet(os.path.join(staging_dir, "audit_rejected_records.parquet"), index=False)

    summary = {
        "valid_transactions": len(fact_transactions),
        "rejected_records": len(audit_table),
        "unique_products": len(dim_products),
        "unique_customers": len(dim_customers)
    }
    logger.info(f"Transformación completada con éxito: {summary}")
    return summary