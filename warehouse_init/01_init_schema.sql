-- ============================================================================
-- DATAMART S.A.S. - DATA WAREHOUSE STAR SCHEMA (DDL IDEMPOTENTE)
-- Se ejecuta automáticamente al iniciar el contenedor warehouse-postgres
-- ============================================================================

-- 1. DIMENSIÓN DE FECHA (Análisis temporal por año, trimestre, mes y día)
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,          -- Formato YYYYMMDD (ej. 20101201)
    full_date DATE NOT NULL UNIQUE,        -- Fecha calendario estandarizada (UTC)
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    day INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,          -- 1 (Lunes) a 7 (Domingo)
    is_weekend BOOLEAN NOT NULL
);

-- 2. DIMENSIÓN DE PRODUCTOS (Catálogo consolidado y enriquecido)
CREATE TABLE IF NOT EXISTS dim_products (
    stock_code VARCHAR(50) PRIMARY KEY,    -- Normalizado: MAYÚSCULAS y sin espacios
    canonical_description TEXT NOT NULL,   -- Nombre canónico elegido (moda estadística)
    category VARCHAR(50) NOT NULL,         -- Electrónica, Hogar, Ropa, Deportes, Papelería
    supplier_country VARCHAR(100) DEFAULT 'United Kingdom',
    is_active BOOLEAN DEFAULT TRUE,
    raw_variation_count INTEGER DEFAULT 1, -- N° de descripciones distintas en los CSV (Pregunta 6)
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 3. DIMENSIÓN DE CLIENTES (Identificados vs. No Identificados - Pregunta 5)
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id VARCHAR(50) PRIMARY KEY,   -- ID del cliente o 'UNIDENTIFIED'
    is_identified BOOLEAN NOT NULL,        -- TRUE si tiene ID válido, FALSE si es 'UNIDENTIFIED'
    primary_country VARCHAR(100) NOT NULL, -- País principal del cliente
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 4. TABLA DE HECHOS TRANSACCIONAL (Grano: línea de factura validada)
CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_sk BIGSERIAL PRIMARY KEY,
    transaction_hash VARCHAR(64) NOT NULL UNIQUE, -- Clave de idempotencia (impide duplicados al re-ejecutar el DAG)
    invoice_no VARCHAR(50) NOT NULL,
    stock_code VARCHAR(50) NOT NULL REFERENCES dim_products(stock_code),
    customer_id VARCHAR(50) NOT NULL REFERENCES dim_customers(customer_id),
    date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
    invoice_timestamp_utc TIMESTAMP WITH TIME ZONE NOT NULL,
    country VARCHAR(100) NOT NULL,
    transaction_type VARCHAR(20) NOT NULL CHECK (transaction_type IN ('SALE', 'RETURN')),
    quantity INTEGER NOT NULL,                    -- Positivo en SALE, <= 0 en RETURN
    unit_price NUMERIC(12, 4) NOT NULL CHECK (unit_price > 0),
    gross_amount NUMERIC(14, 4) NOT NULL,         -- quantity * unit_price
    source_file VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 5. TABLA DE HECHOS AGREGADA DIARIA POR PRODUCTO (Regla de Negocio Sección 5)
-- Almacena el revenue bruto y el revenue neto diario por código de producto
CREATE TABLE IF NOT EXISTS fact_daily_product_metrics (
    date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
    stock_code VARCHAR(50) NOT NULL REFERENCES dim_products(stock_code),
    sales_count INTEGER NOT NULL DEFAULT 0,
    returns_count INTEGER NOT NULL DEFAULT 0,
    units_sold INTEGER NOT NULL DEFAULT 0,
    units_returned INTEGER NOT NULL DEFAULT 0,          -- Valor absoluto de unidades devueltas
    gross_revenue NUMERIC(14, 4) NOT NULL DEFAULT 0.00, -- Suma de ventas brutas del día
    returns_amount NUMERIC(14, 4) NOT NULL DEFAULT 0.00,-- Suma de devoluciones del día (valor positivo)
    net_revenue NUMERIC(14, 4) NOT NULL DEFAULT 0.00,   -- gross_revenue - returns_amount
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date_key, stock_code)                  -- Clave compuesta idempotente por día y producto
);

-- 6. TABLA DE AUDITORÍA DE RECHAZOS (Calidad de Datos - Sección 5 y 6)
CREATE TABLE IF NOT EXISTS audit_rejected_records (
    reject_id BIGSERIAL PRIMARY KEY,
    reject_hash VARCHAR(64) NOT NULL UNIQUE,            -- Clave de idempotencia (evita duplicar rechazos en re-ejecuciones)
    raw_invoice_no TEXT,
    raw_stock_code TEXT,
    raw_description TEXT,
    raw_quantity TEXT,
    raw_unit_price TEXT,
    raw_invoice_date TEXT,
    raw_customer_id TEXT,
    raw_country TEXT,
    rejection_reason VARCHAR(100) NOT NULL,             -- Ej: 'UNIT_PRICE_ZERO_OR_NEGATIVE', 'MISSING_STOCK_CODE'
    source_file VARCHAR(50) NOT NULL,
    rejected_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- ÍNDICES DE RENDIMIENTO PARA LAS CONSULTAS DE NEGOCIO (Sección 7)
-- ============================================================================
CREATE INDEX IF NOT EXISTS idx_fact_trans_date ON fact_transactions(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_trans_product ON fact_transactions(stock_code);
CREATE INDEX IF NOT EXISTS idx_fact_trans_customer ON fact_transactions(customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_trans_type ON fact_transactions(transaction_type);
CREATE INDEX IF NOT EXISTS idx_fact_trans_country ON fact_transactions(country);
CREATE INDEX IF NOT EXISTS idx_audit_reject_reason ON audit_rejected_records(rejection_reason);