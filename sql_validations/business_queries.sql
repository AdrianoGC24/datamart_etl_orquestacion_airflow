-- ==============================================================================
-- DATAMART S.A.S. - BUSINESS VALIDATION QUERIES (SECTION 7)
-- Fully executable against PostgreSQL Data Warehouse (datamart_dw)
-- ==============================================================================

-- ==============================================================================
-- QUESTION 1:
-- Monthly evolution of net sales (discounting returns) during the covered period.
-- ==============================================================================
SELECT
    d.year,
    d.month,
    d.month_name,
    COUNT(CASE WHEN t.transaction_type = 'SALE' THEN 1 END) AS sales_orders_count,
    COUNT(CASE WHEN t.transaction_type = 'RETURN' THEN 1 END) AS return_orders_count,
    ROUND(SUM(CASE WHEN t.transaction_type = 'SALE' THEN t.gross_amount ELSE 0 END), 2) AS gross_sales_amount,
    ROUND(SUM(CASE WHEN t.transaction_type = 'RETURN' THEN ABS(t.gross_amount) ELSE 0 END), 2) AS total_refunds_amount,
    ROUND(SUM(t.gross_amount), 2) AS net_sales_amount
FROM fact_transactions t
JOIN dim_date d ON t.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;


-- ==============================================================================
-- QUESTION 2:
-- Product categories that generated the most gross revenue and had the highest return rate.
-- ==============================================================================
SELECT
    p.category,
    COUNT(CASE WHEN t.transaction_type = 'SALE' THEN 1 END) AS total_sales_lines,
    COUNT(CASE WHEN t.transaction_type = 'RETURN' THEN 1 END) AS total_return_lines,
    ROUND(SUM(CASE WHEN t.transaction_type = 'SALE' THEN t.gross_amount ELSE 0 END), 2) AS gross_revenue,
    ROUND(SUM(CASE WHEN t.transaction_type = 'RETURN' THEN ABS(t.gross_amount) ELSE 0 END), 2) AS return_amount,
    ROUND(SUM(t.gross_amount), 2) AS net_revenue,
    ROUND(
        (SUM(CASE WHEN t.transaction_type = 'RETURN' THEN ABS(t.gross_amount) ELSE 0 END) /
        NULLIF(SUM(CASE WHEN t.transaction_type = 'SALE' THEN t.gross_amount ELSE 0 END), 0) * 100), 2
    ) AS return_rate_pct
FROM fact_transactions t
JOIN dim_products p ON t.stock_code = p.stock_code
GROUP BY p.category
ORDER BY gross_revenue DESC;


-- ==============================================================================
-- QUESTION 3 - PART A:
-- Top 10 products with highest net revenue.
-- ==============================================================================
SELECT
    p.stock_code,
    p.canonical_description AS product_name,
    p.category,
    SUM(m.units_sold) AS total_units_sold,
    ROUND(SUM(m.gross_revenue), 2) AS total_gross_revenue,
    ROUND(SUM(m.returns_amount), 2) AS total_refunds,
    ROUND(SUM(m.net_revenue), 2) AS total_net_revenue
FROM fact_daily_product_metrics m
JOIN dim_products p ON m.stock_code = p.stock_code
GROUP BY p.stock_code, p.canonical_description, p.category
ORDER BY total_net_revenue DESC
LIMIT 10;


-- ==============================================================================
-- QUESTION 3 - PART B:
-- Top 10 products with highest return rate (for products with at least $1,000 gross sales).
-- ==============================================================================
SELECT
    p.stock_code,
    p.canonical_description AS product_name,
    p.category,
    SUM(m.units_sold) AS units_sold,
    SUM(m.units_returned) AS units_returned,
    ROUND(SUM(m.gross_revenue), 2) AS gross_revenue,
    ROUND(SUM(m.returns_amount), 2) AS returns_amount,
    ROUND(
        (SUM(m.returns_amount) / NULLIF(SUM(m.gross_revenue), 0) * 100), 2
    ) AS return_rate_pct
FROM fact_daily_product_metrics m
JOIN dim_products p ON m.stock_code = p.stock_code
GROUP BY p.stock_code, p.canonical_description, p.category
HAVING SUM(m.gross_revenue) >= 1000
ORDER BY return_rate_pct DESC
LIMIT 10;


-- ==============================================================================
-- QUESTION 4:
-- Geographical concentration of transactions and average order ticket variation.
-- ==============================================================================
SELECT
    country,
    COUNT(CASE WHEN transaction_type = 'SALE' THEN 1 END) AS sales_transactions_count,
    ROUND(
        COUNT(CASE WHEN transaction_type = 'SALE' THEN 1 END) * 100.0 /
        SUM(COUNT(CASE WHEN transaction_type = 'SALE' THEN 1 END)) OVER (), 2
    ) AS pct_of_total_transactions,
    ROUND(SUM(CASE WHEN transaction_type = 'SALE' THEN gross_amount ELSE 0 END), 2) AS total_gross_sales,
    ROUND(
        AVG(CASE WHEN transaction_type = 'SALE' THEN gross_amount ELSE NULL END), 2
    ) AS avg_ticket_gross
FROM fact_transactions
GROUP BY country
ORDER BY sales_transactions_count DESC
LIMIT 15;


-- ==============================================================================
-- QUESTION 5:
-- Purchasing behavior comparison: Identified Customers vs. Unidentified / Guest Transactions.
-- ==============================================================================
SELECT
    c.is_identified,
    CASE WHEN c.is_identified THEN 'Identified Customer' ELSE 'Unidentified / Guest' END AS customer_segment,
    COUNT(CASE WHEN t.transaction_type = 'SALE' THEN 1 END) AS sales_transactions_count,
    COUNT(CASE WHEN t.transaction_type = 'RETURN' THEN 1 END) AS returns_transactions_count,
    ROUND(SUM(CASE WHEN t.transaction_type = 'SALE' THEN t.gross_amount ELSE 0 END), 2) AS gross_sales_amount,
    ROUND(AVG(CASE WHEN t.transaction_type = 'SALE' THEN t.gross_amount ELSE NULL END), 2) AS avg_ticket_gross,
    ROUND(AVG(CASE WHEN t.transaction_type = 'SALE' THEN t.quantity ELSE NULL END), 2) AS avg_units_per_order,
    ROUND(
        (SUM(CASE WHEN t.transaction_type = 'RETURN' THEN ABS(t.gross_amount) ELSE 0 END) /
        NULLIF(SUM(CASE WHEN t.transaction_type = 'SALE' THEN t.gross_amount ELSE 0 END), 0) * 100), 2
    ) AS return_rate_pct
FROM fact_transactions t
JOIN dim_customers c ON t.customer_id = c.customer_id
GROUP BY c.is_identified;


-- ==============================================================================
-- QUESTION 6:
-- Inconsistent product descriptions across raw sources and total unique product count.
-- ==============================================================================
-- 6.1 Total unique product codes in the catalog:
SELECT COUNT(DISTINCT stock_code) AS total_unique_product_codes FROM dim_products;

-- 6.2 Products with inconsistent descriptions (> 1 distinct description across sources):
SELECT
    stock_code,
    canonical_description,
    category,
    raw_variation_count AS distinct_description_variations
FROM dim_products
WHERE raw_variation_count > 1
ORDER BY raw_variation_count DESC, stock_code
LIMIT 25;


-- ==============================================================================
-- QUESTION 7:
-- Data-backed strategic recommendation for DataMart's Product & Executive Team.
-- Query backing the recommendation (Top return revenue loss items):
-- ==============================================================================
SELECT
    p.stock_code,
    p.canonical_description,
    p.category,
    SUM(m.units_sold) AS units_sold,
    SUM(m.units_returned) AS units_returned,
    ROUND(SUM(m.gross_revenue), 2) AS gross_revenue,
    ROUND(SUM(m.returns_amount), 2) AS total_loss_refunds,
    ROUND((SUM(m.returns_amount) / NULLIF(SUM(m.gross_revenue), 0) * 100), 2) AS return_rate_pct
FROM fact_daily_product_metrics m
JOIN dim_products p ON m.stock_code = p.stock_code
WHERE p.stock_code IN ('23843', '23166', '85232B', '23113')
GROUP BY p.stock_code, p.canonical_description, p.category
ORDER BY total_loss_refunds DESC;
