
import logging
from pathlib import Path
from datetime import datetime
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import os

BASE = Path(r"C:\Users\sreed\OneDrive\Desktop\Interview")  
RAW_DIR = BASE / "data" / "raw"
REPORTS_DIR = BASE / "reports"
ERRORS_DIR = BASE / "errors"

# Ensure directory existence
for p in (RAW_DIR, REPORTS_DIR, ERRORS_DIR):
    p.mkdir(parents=True, exist_ok=True)

# Load DB credentials from .env (project root)
load_dotenv(dotenv_path=BASE / ".env")
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASS = os.getenv("MYSQL_PASS", "Sreedatta@123")
MYSQL_DB = os.getenv("MYSQL_DB", "divami_sales")

# SQLAlchemy DB URL (mysql+mysqlconnector)
password_quoted = quote_plus(MYSQL_PASS)
SQLALCHEMY_DATABASE_URL = f"mysql+mysqlconnector://{MYSQL_USER}:{password_quoted}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

REQUIRED_COLS = ["timestamp", "product_id", "channel", "quantity", "price_per_unit"]


# ---------------------------
# DB Engine helper
# ---------------------------
def get_engine():
    try:
        engine = create_engine(
            SQLALCHEMY_DATABASE_URL,
            pool_recycle=3600,
            pool_pre_ping=True,
            future=True,
        )
        # quick smoke test
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logging.info("Connected to MySQL successfully.")
        return engine
    except SQLAlchemyError as e:
        logging.exception("Failed to create engine / connect to MySQL. Check credentials / DB availability.")
        raise


# ---------------------------
# READ CSVs
# ---------------------------
def read_all_csvs(raw_dir: Path) -> pd.DataFrame:
    files = list(raw_dir.glob("*.csv"))
    logging.info(f"Found {len(files)} CSV files in {raw_dir}")
    dfs = []
    for f in files:
        try:
            df = pd.read_csv(f)
            df["_source_file"] = f.name
            dfs.append(df)
            logging.info(f"Loaded {f.name} -> {len(df)} rows")
        except Exception:
            logging.exception(f"Failed to read {f}")
    if not dfs:
        return pd.DataFrame(columns=REQUIRED_COLS + ["_source_file"])
    return pd.concat(dfs, ignore_index=True)


# ---------------------------
# VALIDATE SCHEMA
# ---------------------------
def validate_columns(df: pd.DataFrame):
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    return True


# ---------------------------
# CLEANING
# ---------------------------
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(f"Starting cleaning. Initial rows: {len(df)}")

    # cast types safely
    df["timestamp"] = pd.to_datetime(df.get("timestamp"), errors="coerce")
    df["quantity"] = pd.to_numeric(df.get("quantity"), errors="coerce")
    df["price_per_unit"] = pd.to_numeric(df.get("price_per_unit"), errors="coerce")

    # drop exact duplicates
    before_dup = len(df)
    df = df.drop_duplicates()
    logging.info(f"Removed {before_dup - len(df)} duplicate rows")

    # build invalid mask
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

    # remove extreme outliers using IQR on quantity (only if enough rows)
    if not valid_rows.empty and valid_rows["quantity"].notna().sum() >= 3:
        q1 = valid_rows["quantity"].quantile(0.25)
        q3 = valid_rows["quantity"].quantile(0.75)
        iqr = q3 - q1
        upper = q3 + 1.5 * iqr
        before_out = len(valid_rows)
        valid_rows = valid_rows[valid_rows["quantity"] <= upper]
        logging.info(f"Removed {before_out - len(valid_rows)} outlier rows (quantity > {upper})")
    else:
        logging.info("Skipped outlier removal (not enough valid rows)")

    # derive fields
    valid_rows["total_revenue"] = valid_rows["quantity"] * valid_rows["price_per_unit"]
    valid_rows["date"] = valid_rows["timestamp"].dt.date

    logging.info(f"Cleaning complete. Valid rows: {len(valid_rows)}")
    return valid_rows


# ---------------------------
# PERSIST CLEANED -> MySQL staging_sales
# ---------------------------
def persist_staging_mysql(df: pd.DataFrame, engine):
    if df.empty:
        logging.info("No cleaned rows to persist to staging.")
        return

    try:
        # use to_sql to append; method='multi' batches inserts
        df.to_sql(
            name="staging_sales",
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
        )
        logging.info(f"Persisted {len(df)} rows to MySQL table 'staging_sales'")
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

    # write CSV reports locally
    daily_file = REPORTS_DIR / f"daily_sales_{report_date}.csv"
    top_file = REPORTS_DIR / f"top5_products_{report_date}.csv"
    daily.to_csv(daily_file, index=False)
    top5.to_csv(top_file, index=False)
    logging.info(f"Wrote CSV reports: {daily_file}, {top_file}")

    # persist daily aggregates to MySQL reporting table
    try:
        daily.to_sql(
            name="reporting_daily_sales",
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
        )
        logging.info("Saved daily aggregates to MySQL table 'reporting_daily_sales'")
    except Exception:
        logging.exception("Failed to persist reporting_daily_sales to MySQL")
        raise

    return str(daily_file), str(top_file)


# ---------------------------
# MAIN
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

    # Persist staging
    persist_staging_mysql(df_clean, engine)

    # Aggregate & reports for the latest date in this batch
    if not df_clean.empty:
        latest = df_clean["date"].max()
        aggregate_and_store(engine, report_date=latest)

    logging.info("=== ETL (MySQL) completed ===")


if __name__ == "__main__":
    main()
