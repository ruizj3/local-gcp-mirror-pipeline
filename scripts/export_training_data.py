"""Exports the labeled fraud_events table into Parquet/CSV snapshots for the
fraud_detection_transformer repo to train against.

Usage:
    python scripts/export_training_data.py --output-dir ../fraud_detection_transformer/data

    # re-export every 30 minutes so the training snapshot stays fresh as new events land
    python scripts/export_training_data.py --output-dir ../fraud_detection_transformer/data --interval-minutes 30
"""
import argparse
import logging
import os
import time

import pandas as pd
from sqlalchemy import create_engine

DEFAULT_DB_URL = "postgresql+psycopg2://pipeline_user:pipeline_password@localhost:5433/analytics_warehouse"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)


def export(db_url: str, output_dir: str, table: str = "fraud_events") -> None:
    logger.info("Connecting to warehouse and reading table '%s'...", table)
    engine = create_engine(db_url)
    df = pd.read_sql_table(table, engine)

    os.makedirs(output_dir, exist_ok=True)
    parquet_path = os.path.join(output_dir, "fraud_events.parquet")
    csv_path = os.path.join(output_dir, "fraud_events.csv")

    # Parquet is the primary artifact dataset.py reads (columnar, faster to load than CSV);
    # CSV is kept alongside for quick manual inspection.
    df.to_parquet(parquet_path, index=False)
    df.to_csv(csv_path, index=False)

    logger.info("Exported %d rows from '%s' to %s and %s", len(df), table, parquet_path, csv_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export labeled fraud_events to Parquet/CSV.")
    parser.add_argument("--output-dir", default="../fraud_detection_transformer/data")
    parser.add_argument("--db-url", default=DEFAULT_DB_URL)
    parser.add_argument("--table", default="fraud_events")
    parser.add_argument(
        "--interval-minutes", type=float, default=None,
        help="If set, re-runs the export on this cadence instead of exiting after one run.",
    )
    args = parser.parse_args()

    if args.interval_minutes is None:
        export(args.db_url, args.output_dir, args.table)
    else:
        logger.info("Running on a %.1f minute cadence. Press Ctrl+C to stop.", args.interval_minutes)
        while True:
            try:
                export(args.db_url, args.output_dir, args.table)
            except Exception:
                logger.exception("Export failed, will retry on the next cadence tick.")
            time.sleep(args.interval_minutes * 60)
