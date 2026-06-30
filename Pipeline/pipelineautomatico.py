import os
import pandas as pd
import numpy as np
import pyodbc
from pathlib import Path


from pathlib import Path






# =========================================================
# CONFIGURACIÓN
# =========================================================
SERVER = r"ANDY86\SQLEXPRESS"             # Ej: r"DESKTOP-XXXX\SQLEXPRESS"
DATABASE = "QualityClaimsDB"
BASE_DIR = Path(r"C:\Users\thean\Documents\Proyecto data analysis")

RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# SQL Server connection using Windows Authentication
CONN_STR_MASTER = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={SERVER};"
    "DATABASE=master;"
    "Trusted_Connection=yes;"
)

CONN_STR_DB = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    "Trusted_Connection=yes;"
)

df = pd.read_csv(PROCESSED_DIR / "fact_quality_claims_clean.csv")

print(df.columns.tolist())
print(df.head(3))
print(df.dtypes)

print(df.isna().sum())

df = pd.read_csv(PROCESSED_DIR / "fact_quality_claims_clean.csv")

print(
    df[
        df["claim_close_date"].isna()
    ][
        ["claim_id","claim_status","claim_close_date","date_key_close","days_to_close"]
    ].head(10)
)


print(df[["quantity_affected",
          "days_to_close",
          "cost_impact_usd",
          "recurrence_flag"]].head(20))      


# =========================================================
# HELPERS
# =========================================================
def get_connection(use_master=False):
    conn_str = CONN_STR_MASTER if use_master else CONN_STR_DB
    return pyodbc.connect(conn_str, autocommit=True)

def execute_sql(conn, sql_text):
    cursor = conn.cursor()
    cursor.execute(sql_text)
    cursor.close()

def execute_sql_batch(conn, sql_text):
    # Split by GO for convenience
    batches = [b.strip() for b in sql_text.split("GO") if b.strip()]
    cursor = conn.cursor()
    for batch in batches:
        cursor.execute(batch)
    cursor.close()

# =========================================================
# PREPROCESSING
# =========================================================
def clean_dataframe(df):
    df.columns = df.columns.str.strip()
    for col in df.select_dtypes(include="object"):
        df[col] = df[col].str.strip()
    df = df.replace("", np.nan)
    return df

def validate_no_duplicates(df, key_col):
    if key_col in df.columns:
        df = df.drop_duplicates(subset=[key_col])
    return df

def process_dim_generic(file_name, key_col):
    df = pd.read_csv(RAW_DIR / file_name)
    df = clean_dataframe(df)
    df = validate_no_duplicates(df, key_col)
    out_name = file_name.replace(".csv", "_clean.csv")
    df.to_csv(PROCESSED_DIR / out_name, index=False, encoding="utf-8")
    return len(df)

def process_dim_date():
    df = pd.read_csv(RAW_DIR / "dim_date.csv")
    df = clean_dataframe(df)
    df["full_date"] = pd.to_datetime(df["full_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["date_key"] = pd.to_numeric(df["date_key"], errors="coerce").astype("Int64")
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["month"] = pd.to_numeric(df["month"], errors="coerce").astype("Int64")
    df["week_number"] = pd.to_numeric(df["week_number"], errors="coerce").astype("Int64")
    df["is_month_end"] = pd.to_numeric(df["is_month_end"], errors="coerce").fillna(0).astype("Int64")
    df = validate_no_duplicates(df, "date_key")
    df.to_csv(PROCESSED_DIR / "dim_date_clean.csv", index=False, encoding="utf-8")
    return len(df)

def process_fact_claims():
    df = pd.read_csv(RAW_DIR / "fact_quality_claims.csv")
    df = clean_dataframe(df)

    # Dates
    df["claim_open_date"] = pd.to_datetime(df["claim_open_date"], errors="coerce")
    df["claim_close_date"] = pd.to_datetime(df["claim_close_date"], errors="coerce")

    # Numeric fields
    numeric_cols = ["date_key_open", "date_key_close", "cost_impact_usd", "quantity_affected", "days_to_close", "recurrence_flag"]
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Normalize flags
    df["recurrence_flag"] = df["recurrence_flag"].fillna(0).astype("Int64")

    # Remove duplicate claims
    df = validate_no_duplicates(df, "claim_id")

    # Fix impossible negative values
    df.loc[df["cost_impact_usd"] < 0, "cost_impact_usd"] = np.nan
    df.loc[df["quantity_affected"] < 0, "quantity_affected"] = np.nan
    df.loc[df["days_to_close"] < 0, "days_to_close"] = np.nan

    # Date logic
    bad_dates = (df["claim_close_date"].notna()) & (df["claim_close_date"] < df["claim_open_date"])
    df.loc[bad_dates, "claim_close_date"] = pd.NaT
    df.loc[bad_dates, "date_key_close"] = pd.NA
    df.loc[bad_dates, "days_to_close"] = pd.NA

    # Standard categorical values
    valid_status = ["Open", "Closed", "In Analysis", "Pending", "Canceled"]
    valid_severity = ["Low", "Medium", "High", "Critical"]

    df.loc[~df["claim_status"].isin(valid_status), "claim_status"] = "In Analysis"
    df.loc[~df["severity_level"].isin(valid_severity), "severity_level"] = "Medium"

    # Save as SQL-friendly strings for date columns
    df["claim_open_date"] = df["claim_open_date"].dt.strftime("%Y-%m-%d")
    df["claim_close_date"] = df["claim_close_date"].dt.strftime("%Y-%m-%d")

    int_cols = [
        "date_key_open",
        "date_key_close",
        "quantity_affected",
        "days_to_close"
    ]
    
    for col in int_cols:
        df[col] = df[col].astype("Int64")


    df.to_csv(PROCESSED_DIR / "fact_quality_claims_clean.csv", index=False, encoding="utf-8")
    return len(df)

# =========================================================
# SQL DDL
# =========================================================
SQL_CREATE_DATABASE = f"""
IF DB_ID('{DATABASE}') IS NULL
BEGIN
    CREATE DATABASE {DATABASE};
END
"""

SQL_CREATE_TABLES = f"""
USE {DATABASE};

IF OBJECT_ID('dim_customer', 'U') IS NULL
CREATE TABLE dim_customer (
    customer_id VARCHAR(10) PRIMARY KEY,
    customer_name VARCHAR(100),
    customer_region VARCHAR(50),
    customer_segment VARCHAR(30),
    industry_segment VARCHAR(50),
    strategic_account_flag BIT
);

IF OBJECT_ID('dim_product', 'U') IS NULL
CREATE TABLE dim_product (
    product_id VARCHAR(10) PRIMARY KEY,
    product_family VARCHAR(50),
    product_group VARCHAR(50),
    specification VARCHAR(50),
    diameter_group VARCHAR(20),
    grade VARCHAR(20),
    application_type VARCHAR(30)
);

IF OBJECT_ID('dim_supplier', 'U') IS NULL
CREATE TABLE dim_supplier (
    supplier_id VARCHAR(10) PRIMARY KEY,
    supplier_name VARCHAR(100),
    supplier_category VARCHAR(50),
    supplier_region VARCHAR(50),
    critical_supplier_flag BIT
);

IF OBJECT_ID('dim_root_cause', 'U') IS NULL
CREATE TABLE dim_root_cause (
    root_cause_id VARCHAR(10) PRIMARY KEY,
    root_cause_category VARCHAR(50),
    root_cause_subcategory VARCHAR(100),
    controllable_flag BIT,
    process_stage VARCHAR(50)
);

IF OBJECT_ID('dim_location', 'U') IS NULL
CREATE TABLE dim_location (
    location_id VARCHAR(10) PRIMARY KEY,
    region VARCHAR(50),
    mill VARCHAR(50),
    country VARCHAR(50),
    plant_type VARCHAR(30)
);

IF OBJECT_ID('dim_date', 'U') IS NULL
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE,
    year INT,
    quarter VARCHAR(2),
    month INT,
    month_name VARCHAR(20),
    week_number INT,
    day_of_week VARCHAR(20),
    is_month_end BIT
);

IF OBJECT_ID('fact_quality_claims', 'U') IS NULL
CREATE TABLE fact_quality_claims (
    claim_id VARCHAR(20) PRIMARY KEY,
    claim_open_date DATE,
    claim_close_date DATE,
    date_key_open INT,
    date_key_close INT,
    customer_id VARCHAR(10),
    product_id VARCHAR(10),
    supplier_id VARCHAR(10),
    root_cause_id VARCHAR(10),
    location_id VARCHAR(10),
    claim_status VARCHAR(20),
    severity_level VARCHAR(20),
    cost_impact_usd DECIMAL(15,2),
    quantity_affected INT,
    days_to_close INT,
    recurrence_flag BIT,
    claim_type VARCHAR(30),
    inspection_stage VARCHAR(30),
    defect_code VARCHAR(10),
    detected_by VARCHAR(30),
    business_unit VARCHAR(30)
);

-- Staging table for future incremental updates (optional but career-valuable)
IF OBJECT_ID('staging_quality_claims', 'U') IS NULL
CREATE TABLE staging_quality_claims (
    claim_id VARCHAR(20),
    claim_open_date DATE,
    claim_close_date DATE,
    date_key_open INT,
    date_key_close INT,
    customer_id VARCHAR(10),
    product_id VARCHAR(10),
    supplier_id VARCHAR(10),
    root_cause_id VARCHAR(10),
    location_id VARCHAR(10),
    claim_status VARCHAR(20),
    severity_level VARCHAR(20),
    cost_impact_usd DECIMAL(15,2),
    quantity_affected INT,
    days_to_close INT,
    recurrence_flag BIT,
    claim_type VARCHAR(30),
    inspection_stage VARCHAR(30),
    defect_code VARCHAR(10),
    detected_by VARCHAR(30),
    business_unit VARCHAR(30)
);
"""

# =========================================================
# BULK INSERT HELPERS
# =========================================================
def windows_path_for_sql(path_obj):
    # SQL Server likes a normal Windows path string
    return str(path_obj).replace("/", "\\")

def bulk_insert_sql(table_name, file_path, firstrow=2):
    fp = windows_path_for_sql(file_path)
    return f"""
USE {DATABASE};
BULK INSERT {table_name}
FROM '{fp}'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0D0A',
    FIELDQUOTE = '"',
    CODEPAGE = '65001',
    TABLOCK
);
"""

#def count_rows(conn, table_name):
#    cur = conn.cursor()
#    cur.execute(f"USE {DATABASE}; SELECT COUNT(*) FROM {table_name};")
#    count = cur.fetchone()[0]
#    cur.close()
#    return count

def count_rows(conn, table_name):
    cur = conn.cursor()

    cur.execute(f"SELECT COUNT(*) FROM {table_name}")

    count = cur.fetchone()[0]

    cur.close()

    return count





# =========================================================
# LOAD ORDER
# =========================================================
LOAD_MAP = [
    ("dim_customer", PROCESSED_DIR / "dim_customer_clean.csv"),
    ("dim_product", PROCESSED_DIR / "dim_product_clean.csv"),
    ("dim_supplier", PROCESSED_DIR / "dim_supplier_clean.csv"),
    ("dim_root_cause", PROCESSED_DIR / "dim_root_cause_clean.csv"),
    ("dim_location", PROCESSED_DIR / "dim_location_clean.csv"),
    ("dim_date", PROCESSED_DIR / "dim_date_clean.csv"),
    ("fact_quality_claims", PROCESSED_DIR / "fact_quality_claims_clean.csv"),
]

# =========================================================
# MAIN
# =========================================================
def run_preprocessing():
    print("🔵 Preprocessing raw CSVs...")
    n1 = process_dim_generic("dim_customer.csv", "customer_id")
    n2 = process_dim_generic("dim_product.csv", "product_id")
    n3 = process_dim_generic("dim_supplier.csv", "supplier_id")
    n4 = process_dim_generic("dim_root_cause.csv", "root_cause_id")
    n5 = process_dim_generic("dim_location.csv", "location_id")
    n6 = process_dim_date()
    n7 = process_fact_claims()
    print(f"✅ Preprocessing completed. Rows saved: customer={n1}, product={n2}, supplier={n3}, root_cause={n4}, location={n5}, date={n6}, claims={n7}")

def run_sql_setup():
    print("🔵 Creating database and tables...")
    with get_connection(use_master=True) as conn:
        execute_sql(conn, SQL_CREATE_DATABASE)
    with get_connection(use_master=False) as conn:
        execute_sql_batch(conn, SQL_CREATE_TABLES)
    print("✅ Database and tables ready.")

def truncate_tables():
    print("🔵 Truncating target tables before reload...")
    sql = f"""
    USE {DATABASE};
    DELETE FROM fact_quality_claims;
    DELETE FROM dim_customer;
    DELETE FROM dim_product;
    DELETE FROM dim_supplier;
    DELETE FROM dim_root_cause;
    DELETE FROM dim_location;
    DELETE FROM dim_date;
    """
    with get_connection(use_master=False) as conn:
        execute_sql_batch(conn, sql)
    print("✅ Tables cleaned.")

def run_bulk_load():
    print("🔵 Loading clean CSVs with BULK INSERT...")
    with get_connection(use_master=False) as conn:
        for table_name, file_path in LOAD_MAP:
            sql = bulk_insert_sql(table_name, file_path)
            execute_sql_batch(conn, sql)
            rows = count_rows(conn, table_name)
            print(f"   - {table_name}: {rows} rows")
    print("✅ BULK INSERT load completed.")

def run_postload_validations():
    print("🔵 Running post-load validations...")
    validations = [
        ("dim_customer", "SELECT COUNT(*) AS rows_count FROM dim_customer"),
        ("dim_product", "SELECT COUNT(*) AS rows_count FROM dim_product"),
        ("dim_supplier", "SELECT COUNT(*) AS rows_count FROM dim_supplier"),
        ("dim_root_cause", "SELECT COUNT(*) AS rows_count FROM dim_root_cause"),
        ("dim_location", "SELECT COUNT(*) AS rows_count FROM dim_location"),
        ("dim_date", "SELECT COUNT(*) AS rows_count FROM dim_date"),
        ("fact_quality_claims", "SELECT COUNT(*) AS rows_count FROM fact_quality_claims"),
        ("duplicate_claims", """
            SELECT COUNT(*) 
            FROM (
                SELECT claim_id
                FROM fact_quality_claims
                GROUP BY claim_id
                HAVING COUNT(*) > 1
            ) d
        """),
        ("closed_without_close_date", """
            SELECT COUNT(*) 
            FROM fact_quality_claims
            WHERE claim_status = 'Closed' AND claim_close_date IS NULL
        """),
    ]

    with get_connection(use_master=False) as conn:
        cur = conn.cursor()
        cur.execute(f"USE {DATABASE};")
        for name, q in validations:
            cur.execute(q)
            val = cur.fetchone()[0]
            print(f"   - {name}: {val}")
        cur.close()
    print("✅ Validations completed.")

def main():
    run_preprocessing()
    run_sql_setup()
    truncate_tables()
    run_bulk_load()
    run_postload_validations()
    print("\n🎯 Pipeline end-to-end completed successfully.")

if __name__ == "__main__":
    main()
