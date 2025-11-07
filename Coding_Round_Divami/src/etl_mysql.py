# src/etl_mysql.py
"""
Complete ETL pipeline (MySQL backend)
- Reads CSVs from BASE / data / raw
- Cleans data: schema validation, missing/negative checks, duplicates, outliers
- Persists cleaned rows into MySQL staging table
- Aggregates daily channel metrics and top-5 products
- Writes CSV reports to BASE / reports and stores aggregates in MySQL reporting table
- Writes invalid rows to BASE / errors
- Ensures required tables exist (DDL)
"""

import logging
from pathlib import Path
from datetime import datetime
from urllib.parse import quote_plus
import os
from io import StringIO

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

# ---------------------------
# CONFIG
# ---------------------------
BASE = Path(r"C:\Users\sreed\OneDrive\Desktop\Interview")  # adjust if needed
RAW_DIR = BASE / "data" / "raw"
REPORTS_DIR = BASE / "reports"
ERRORS_DIR = BASE / "errors"

# Ensure directories
for p in (RAW_DIR, REPORTS_DIR, ERRORS_DIR):
    p.mkdir(parents=True, exist_ok=True)

# Load environment and DB creds
load_dotenv(dotenv_path=BASE / ".env")
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
MYSQL_USER = os.getenv("MYSQL_USER", "divami_user")
MYSQL_PASS = os.getenv("MYSQL_PASS", "divami_pass")
MYSQL_DB = os.getenv("MYSQL_DB", "divami_sales")

# SQLAlchemy DB URL using PyMySQL (works reliably in this environment)
password_quoted = quote_plus(MYSQL_PASS)
SQLALCHEMY_DATABASE_URL = f"mysql+pymysql://{MYSQL_USER}:{password_quoted}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

REQUIRED_COLS = ["timestamp", "product_id", "channel", "quantity", "price_per_unit"]


# ---------------------------
# DB helpers
# ---------------------------
def get_engine():
    try:
        engine = create_engine(
            SQLALCHEMY_DATABASE_URL,
            pool_recycle=3600,
            pool_pre_ping=True,
            future=True,
        )
        # smoke test
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logging.info("Connected to MySQL successfully.")
        return engine
    except SQLAlchemyError:
        logging.exception("Failed to create engine / connect to MySQL. Check credentials / DB availability.")
        raise


def ensure_tables(engine):
    """
    Create staging and reporting tables if they don't exist.
    Idempotent; safe to call on each run.
    """
    ddl = f"""
    CREATE DATABASE IF NOT EXISTS {MYSQL_DB};
    USE {MYSQL_DB};

    CREATE TABLE IF NOT EXISTS staging_sales (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      timestamp DATETIME NOT NULL,
      product_id VARCHAR(128) NOT NULL,
      channel VARCHAR(64) NOT NULL,
      quantity INT NOT NULL,
      price_per_unit DECIMAL(12,2) NOT NULL,
      total_revenue DECIMAL(14,2) NOT NULL,
      date DATE NOT NULL,
      source_file VARCHAR(256),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS reporting_daily_sales (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      report_date DATE NOT NULL,
      channel VARCHAR(64) NOT NULL,
      total_sales BIGINT NOT NULL,
      total_revenue DECIMAL(18,2) NOT NULL,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      UNIQUE KEY uq_report_date_channel (report_date, channel)
    );
    """
    try:
        with engine.begin() as conn:
            for stmt in [s.strip() for s in ddl.split(";") if s.strip()]:
                conn.execute(text(stmt))
        logging.info("Ensured required tables exist (staging_sales, reporting_daily_sales).")
    except Exception:
        logging.exception("Failed to ensure DDL tables.")
        raise


# ---------------------------
# CSV reading (robust)
# ---------------------------
def read_all_csvs(raw_dir: Path) -> pd.DataFrame:
    """
    Read all CSVs in raw_dir. Strips inline comments that start with '#'
    (naive approach suitable for interview/demo CSVs).
    """
    files = sorted(raw_dir.glob("*.csv"))
    logging.info(f"Found {len(files)} CSV files in {raw_dir}")
    dfs = []
    for f in files:
        try:
            # read as text and strip inline comments (after '#')
            with open(f, "r", encoding="utf-8") as fh:
                lines = []
                for line in fh:
                    if '#' in line:
                        # remove content after first '#'
                        line = line.split("#", 1)[0].rstrip() + "\n"
                    lines.append(line)
            cleaned_text = "".join(lines).strip()
            if not cleaned_text:
                logging.warning(f"{f.name} cleaned to empty content; skipping.")
                continue
            df = pd.read_csv(StringIO(cleaned_text))
            df["_source_file"] = f.name
            dfs.append(df)
            logging.info(f"Loaded {f.name} -> {len(df)} rows (after cleaning comments)")
        except Exception:
            logging.exception(f"Failed to read {f}")
    if not dfs:
        return pd.DataFrame(columns=REQUIRED_COLS + ["_source_file"])
    combined = pd.concat(dfs, ignore_index=True)
    return combined


# ---------------------------
# Validation & Cleaning
# ---------------------------
def validate_columns(df: pd.DataFrame):
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    return True


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(f"Starting cleaning. Initial rows: {len(df)}")
    # coerce types
    df["timestamp"] = pd.to_datetime(df.get("timestamp"), errors="coerce")
    df["quantity"] = pd.to_numeric(df.get("quantity"), errors="coerce")
    df["price_per_unit"] = pd.to_numeric(df.get("price_per_unit"), errors="coerce")

    # drop exact duplicates
    before_dup = len(df)
    df = df.drop_duplicates().reset_index(drop=True)
    logging.info(f"Removed {before_dup - len(df)} duplicate rows")

    # invalid mask
    invalid_mask = (
        df["timestamp"].isna()
        | df["product_id"].isna()
        | df["channel"].isna()
        | df["quantity"].isna()
        | df["price_per_unit"].isna()
        | (df["quantity"] <= 0)
        | (df["price_per_unit"] < 0)
    )

    invalid_rows = df[invalid_mask].copy()
    valid_rows = df[~invalid_mask].copy()

    # persist invalid rows for auditing
    if not invalid_rows.empty:
        err_file = ERRORS_DIR / f"invalid_rows_{datetime.utcnow():%Y%m%d_%H%M%S}.csv"
        invalid_rows.to_csv(err_file, index=False)
        logging.warning(f"Wrote {len(invalid_rows)} invalid rows to {err_file}")

    # work on a real copy to avoid SettingWithCopyWarning
    if not valid_rows.empty:
        valid_rows = valid_rows.copy()
    else:
        logging.info("No valid rows after cleaning.")
        return valid_rows

    # remove extreme outliers using IQR on quantity if enough rows
    if valid_rows["quantity"].notna().sum() >= 3:
        q1 = valid_rows["quantity"].quantile(0.25)
        q3 = valid_rows["quantity"].quantile(0.75)
        iqr = q3 - q1
        upper = q3 + 1.5 * iqr
        before_out = len(valid_rows)
        valid_rows = valid_rows[valid_rows["quantity"] <= upper].reset_index(drop=True)
        logging.info(f"Removed {before_out - len(valid_rows)} outlier rows (quantity > {upper})")
    else:
        logging.info("Skipped outlier removal (not enough valid rows)")

    # derived fields
    valid_rows["total_revenue"] = valid_rows["quantity"] * valid_rows["price_per_unit"]
    valid_rows["date"] = valid_rows["timestamp"].dt.date

    logging.info(f"Cleaning complete. Valid rows: {len(valid_rows)}")
    return valid_rows


# ---------------------------
# Persist & Aggregate
# ---------------------------
def persist_staging_mysql(df: pd.DataFrame, engine):
    """
    Persist cleaned rows to staging_sales.
    Ensures DataFrame column names match the DB schema (rename or drop unexpected cols).
    Returns number of rows persisted.
    """
    if df.empty:
        logging.info("No cleaned rows to persist to staging.")
        return 0

    # make a defensive copy
    to_write = df.copy()

    # rename _source_file -> source_file if present (this is the column created during CSV load)
    if "_source_file" in to_write.columns and "source_file" not in to_write.columns:
        to_write = to_write.rename(columns={"_source_file": "source_file"})
        logging.info("Renamed column _source_file -> source_file to match DB schema.")

    # Drop any columns that are not in the DB schema (safe whitelist)
    allowed = {"timestamp", "product_id", "channel", "quantity", "price_per_unit", "total_revenue", "date", "source_file"}
    extra_cols = [c for c in to_write.columns if c not in allowed]
    if extra_cols:
        logging.info(f"Dropping extra columns before DB persist: {extra_cols}")
        to_write = to_write[[c for c in to_write.columns if c in allowed]]

    try:
        # append to staging_sales (table created in ensure_tables)
        to_write.to_sql(name="staging_sales", con=engine, if_exists="append", index=False, method="multi")
        logging.info(f"Persisted {len(to_write)} rows to MySQL table 'staging_sales'")
        return len(to_write)
    except Exception:
        logging.exception("Failed to persist staging data to MySQL")
        raise


# ---------------------------
# AGGREGATE & WRITE REPORTS (CSV + MySQL reporting table)
# ---------------------------
def aggregate_and_store(engine, report_date=None):
    try:
        df = pd.read_sql_query("SELECT * FROM staging_sales", con=engine, parse_dates=["timestamp"])
    except Exception:
        logging.exception("Failed to read staging_sales from MySQL")
        raise

    if df.empty:
        logging.warning("staging_sales is empty; skipping aggregation")
        return None

    # ensure date column typed correctly (if read back as string)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"]).dt.date
    else:
        df["date"] = pd.to_datetime(df["timestamp"]).dt.date

    if report_date is None:
        report_date = df["date"].max()

    logging.info(f"Aggregating for date: {report_date}")

    # daily aggregates by channel
    daily = (
        df[df["date"] == pd.to_datetime(report_date).date()]
        .groupby(["date", "channel"], as_index=False)
        .agg(total_sales=("quantity", "sum"), total_revenue=("total_revenue", "sum"))
    )

    # top 5 products by quantity
    top5 = (
        df[df["date"] == pd.to_datetime(report_date).date()]
        .groupby(["product_id"], as_index=False)
        .agg(total_sales=("quantity", "sum"), total_revenue=("total_revenue", "sum"))
        .sort_values(["total_sales", "total_revenue"], ascending=False)
        .head(5)
    )

    # write CSV reports locally (unchanged)
    report_date_str = pd.to_datetime(report_date).date()
    daily_file = REPORTS_DIR / f"daily_sales_{report_date_str}.csv"
    top_file = REPORTS_DIR / f"top5_products_{report_date_str}.csv"
    daily.to_csv(daily_file, index=False)
    top5.to_csv(top_file, index=False)
    logging.info(f"Wrote CSV reports: {daily_file}, {top_file}")

    # persist daily aggregates to MySQL reporting table
    # rename 'date' -> 'report_date' to match DB column name
    daily_db = daily.rename(columns={"date": "report_date"}).copy()

    # ensure columns are exactly the expected ones (whitelist)
    expected_cols = ["report_date", "channel", "total_sales", "total_revenue"]
    extra = [c for c in daily_db.columns if c not in expected_cols]
    if extra:
        logging.info(f"Dropping unexpected cols before writing aggregates: {extra}")
        daily_db = daily_db[[c for c in daily_db.columns if c in expected_cols]]

    try:
        with engine.begin() as conn:
            for _, row in daily_db.iterrows():
                conn.execute(text(
                    """
                    INSERT INTO reporting_daily_sales (report_date, channel, total_sales, total_revenue)
                    VALUES (:report_date, :channel, :total_sales, :total_revenue)
                    ON DUPLICATE KEY UPDATE
                      total_sales = VALUES(total_sales),
                      total_revenue = VALUES(total_revenue),
                      updated_at = CURRENT_TIMESTAMP
                    """
                ), {
                    "report_date": str(row["report_date"]),
                    "channel": row["channel"],
                    "total_sales": int(row["total_sales"]),
                    "total_revenue": float(row["total_revenue"]),
                })
        logging.info("Saved daily aggregates to MySQL table 'reporting_daily_sales' (upserted)")
    except Exception:
        logging.exception("Failed to persist reporting_daily_sales to MySQL")
        raise

    return str(daily_file), str(top_file)


# ---------------------------
# Main
# ---------------------------
def main():
    logging.info("=== ETL (MySQL) started ===")

    # Read
    df_raw = read_all_csvs(RAW_DIR)
    if df_raw.empty:
        logging.warning("No input files found in raw directory. Exiting.")
        return

    # Validate
    try:
        validate_columns(df_raw)
    except ValueError:
        logging.exception("Schema validation failed - aborting ETL.")
        return

    # Clean
    df_clean = clean_data(df_raw)

    # DB connect
    try:
        engine = get_engine()
    except Exception:
        logging.error("Cannot proceed without DB connection.")
        return

    # Ensure tables exist (safe)
    try:
        ensure_tables(engine)
    except Exception:
        logging.error("Cannot ensure DB tables; aborting.")
        return

    # Persist staging
    try:
        persisted = persist_staging_mysql(df_clean, engine)
    except Exception:
        logging.error("Persist to staging failed; aborting.")
        return

    # Aggregate & reports for the latest date in this batch
    if persisted and not df_clean.empty:
        latest = df_clean["date"].max()
        try:
            aggregate_and_store(engine, report_date=latest)
        except Exception:
            logging.error("Aggregation/reporting failed.")
            return
    else:
        logging.info("No rows persisted; skipping aggregation.")

    logging.info("=== ETL (MySQL) completed ===")


if __name__ == "__main__":
    main()
