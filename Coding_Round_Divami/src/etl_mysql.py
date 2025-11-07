import logging
from pathlib import Path
from datetime import datetime
from urllib.parse import quote_plus
import os
from io import StringIO
from typing import Optional, Tuple

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

# ----- Configuration -----
BASE = Path(r"C:\Users\sreed\OneDrive\Desktop\Interview")
RAW_DIR = BASE / "data" / "raw"
REPORTS_DIR = BASE / "reports"
ERRORS_DIR = BASE / "errors"
for d in (RAW_DIR, REPORTS_DIR, ERRORS_DIR):
    d.mkdir(parents=True, exist_ok=True)

load_dotenv(dotenv_path=BASE / ".env")
DB_HOST = os.getenv("MYSQL_HOST", "localhost")
DB_PORT = os.getenv("MYSQL_PORT", "3306")
DB_USER = os.getenv("MYSQL_USER", "divami_user")
DB_PASS = os.getenv("MYSQL_PASS", "divami_pass")
DB_NAME = os.getenv("MYSQL_DB", "divami_sales")

DB_URL = f"mysql+pymysql://{DB_USER}:{quote_plus(DB_PASS)}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

REQUIRED_COLUMNS = ["timestamp", "product_id", "channel", "quantity", "price_per_unit"]
STAGING_COLUMNS = ["timestamp", "product_id", "channel", "quantity", "price_per_unit", "total_revenue", "date", "source_file"]


# ----- DB helpers -----
def get_engine():
    """Create SQLAlchemy engine and smoke-test it."""
    try:
        engine = create_engine(DB_URL, pool_recycle=3600, pool_pre_ping=True, future=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logging.info("DB OK")
        return engine
    except SQLAlchemyError:
        logging.exception("DB connection failed")
        raise


def ensure_db_tables(engine):
    """Create the DB and the two tables if missing (idempotent)."""
    ddl = f"""
    CREATE DATABASE IF NOT EXISTS {DB_NAME};
    USE {DB_NAME};
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
    with engine.begin() as conn:
        for stmt in [s.strip() for s in ddl.split(";") if s.strip()]:
            conn.execute(text(stmt))
    logging.info("Ensured tables are present")


# ----- CSV reading & simple pre-processing -----
def _strip_comments_from_text(text: str) -> str:
  
    out_lines = []
    for line in text.splitlines():
        if "#" in line:
            line = line.split("#", 1)[0].rstrip()
        if line:  # skip empty lines after stripping
            out_lines.append(line)
    return "\n".join(out_lines)


def load_raw_csvs(raw_dir: Path) -> pd.DataFrame:
    """Load all CSVs from raw_dir, add a source file column."""
    files = sorted(raw_dir.glob("*.csv"))
    logging.info(f"Found {len(files)} csv(s) in {raw_dir}")
    frames = []
    for f in files:
        try:
            raw = f.read_text(encoding="utf-8")
            cleaned = _strip_comments_from_text(raw)
            if not cleaned:
                logging.warning(f"{f.name} empty after stripping comments; skipping")
                continue
            df = pd.read_csv(StringIO(cleaned))
            df["_source_file"] = f.name
            frames.append(df)
            logging.info(f"Loaded {f.name} ({len(df)} rows)")
        except Exception:
            logging.exception(f"Failed loading {f.name}")
    if not frames:
        return pd.DataFrame(columns=REQUIRED_COLUMNS + ["_source_file"])
    return pd.concat(frames, ignore_index=True)


# ----- Cleaning -----
def clean_records(df: pd.DataFrame) -> pd.DataFrame:
    """Validate columns, coerce types, drop bad rows, remove outliers, compute totals."""
    logging.info(f"Cleaning {len(df)} input rows")
    # required schema
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # coercions
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["price_per_unit"] = pd.to_numeric(df["price_per_unit"], errors="coerce")

    # drop duplicates & obvious bads
    before = len(df)
    df = df.drop_duplicates().reset_index(drop=True)
    logging.info(f"dropped {before - len(df)} duplicate rows")

    bad_mask = (
        df["timestamp"].isna()
        | df["product_id"].isna()
        | df["channel"].isna()
        | df["quantity"].isna()
        | df["price_per_unit"].isna()
        | (df["quantity"] <= 0)
        | (df["price_per_unit"] < 0)
    )
    bad = df[bad_mask].copy()
    good = df[~bad_mask].copy()

    if not bad.empty:
        fn = ERRORS_DIR / f"invalid_rows_{datetime.utcnow():%Y%m%d_%H%M%S}.csv"
        bad.to_csv(fn, index=False)
        logging.warning(f"Wrote {len(bad)} invalid rows to {fn}")

    # outlier removal on quantity (IQR)
    if good["quantity"].notna().sum() >= 3:
        q1 = good["quantity"].quantile(0.25)
        q3 = good["quantity"].quantile(0.75)
        iqr = q3 - q1
        upper = q3 + 1.5 * iqr
        before_out = len(good)
        good = good[good["quantity"] <= upper].reset_index(drop=True)
        logging.info(f"removed {before_out - len(good)} outlier(s) (quantity > {upper})")

    # derived fields
    good = good.copy()
    good["total_revenue"] = good["quantity"] * good["price_per_unit"]
    good["date"] = good["timestamp"].dt.date

    logging.info(f"Cleaning complete: {len(good)} valid rows")
    return good


# ----- Persistence -----
def write_staging(df: pd.DataFrame, engine) -> int:
    """Write cleaned rows into staging_sales — rename/drop columns to match table schema."""
    if df.empty:
        logging.info("Nothing to write to staging")
        return 0

    to_write = df.copy()
    # rename the field we added at read-time
    if "_source_file" in to_write.columns:
        to_write = to_write.rename(columns={"_source_file": "source_file"})
        logging.info("Renamed _source_file -> source_file")

    # enforce columns we expect
    allowed = set(STAGING_COLUMNS)
    present = [c for c in to_write.columns if c in allowed]
    to_write = to_write[present]

    try:
        to_write.to_sql(name="staging_sales", con=engine, if_exists="append", index=False, method="multi")
        logging.info(f"Wrote {len(to_write)} rows to staging_sales")
        return len(to_write)
    except Exception:
        logging.exception("Failed writing to staging")
        raise


# ----- Aggregation & reporting -----
def generate_reports(engine, report_date: Optional[datetime.date] = None) -> Optional[Tuple[str, str]]:
    """Read from staging_sales, produce daily and top-5 reports, upsert aggregates to reporting table."""
    try:
        df = pd.read_sql_query("SELECT * FROM staging_sales", con=engine, parse_dates=["timestamp"])
    except Exception:
        logging.exception("Failed to read staging_sales")
        raise

    if df.empty:
        logging.info("staging_sales empty — skipping report generation")
        return None

    # normalize date column
    df["date"] = pd.to_datetime(df.get("date", df["timestamp"])).dt.date

    if report_date is None:
        report_date = df["date"].max()
    logging.info(f"Building reports for {report_date}")

    # daily metrics
    daily = (
        df[df["date"] == pd.to_datetime(report_date).date()]
        .groupby(["date", "channel"], as_index=False)
        .agg(total_sales=("quantity", "sum"), total_revenue=("total_revenue", "sum"))
    )

    # top 5 products
    top5 = (
        df[df["date"] == pd.to_datetime(report_date).date()]
        .groupby(["product_id"], as_index=False)
        .agg(total_sales=("quantity", "sum"), total_revenue=("total_revenue", "sum"))
        .sort_values(["total_sales", "total_revenue"], ascending=False)
        .head(5)
    )

    # write CSVs
    date_str = pd.to_datetime(report_date).date()
    daily_path = REPORTS_DIR / f"daily_sales_{date_str}.csv"
    top5_path = REPORTS_DIR / f"top5_products_{date_str}.csv"
    daily.to_csv(daily_path, index=False)
    top5.to_csv(top5_path, index=False)
    logging.info(f"Wrote reports: {daily_path.name}, {top5_path.name}")

    # upsert aggregates into reporting_daily_sales
    daily_upsert = daily.rename(columns={"date": "report_date"}).copy()
    expected = ["report_date", "channel", "total_sales", "total_revenue"]
    daily_upsert = daily_upsert[[c for c in expected if c in daily_upsert.columns]]

    try:
        with engine.begin() as conn:
            for _, row in daily_upsert.iterrows():
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
        logging.info("Upserted daily aggregates to reporting_daily_sales")
    except Exception:
        logging.exception("Failed to upsert aggregates")
        raise

    return str(daily_path), str(top5_path)


# ----- Main flow -----
def main():
    logging.info("ETL started")
    raw = load_raw_csvs(RAW_DIR)
    if raw.empty:
        logging.warning("No input files - exiting")
        return

    try:
        valid = clean_records(raw)
    except Exception:
        logging.exception("Cleaning failed - aborting")
        return

    try:
        engine = get_engine()
    except Exception:
        return

    try:
        ensure_db_tables(engine)
    except Exception:
        return

    try:
        count = write_staging(valid, engine)
    except Exception:
        logging.error("Failed writing staging - abort")
        return

    if count:
        try:
            generate_reports(engine, report_date=valid["date"].max())
        except Exception:
            logging.error("Report generation failed")
            return

    logging.info("ETL completed")


if __name__ == "__main__":
    main()

