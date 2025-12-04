import logging
import sqlite3
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def load_to_sqlite(
    input_path: str = "data/clean_products.csv",
    db_path: str = "data/output.db",
    table_name: str = "products",
) -> None:
    """
    Load cleaned Sulpak products into a SQLite DB.
    """
    input_file = Path(input_path)
    if not input_file.exists():
        raise FileNotFoundError(f"Cleaned data file not found: {input_file}")

    df = pd.read_csv(input_file)
    logger.info("Loaded %d cleaned rows from %s", len(df), input_file)

    db_file = Path(db_path)
    db_file.parent.mkdir(parents=True, exist_ok=True)

    conn: Optional[sqlite3.Connection] = None
    try:
        conn = sqlite3.connect(db_file)
        # You can also create table manually, but pandas.to_sql is fine here
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        logger.info("Inserted %d rows into table '%s' in %s", len(df), table_name, db_file)
    finally:
        if conn is not None:
            conn.close()


def run_loader() -> None:
    """Entry point for Airflow / CLI."""
    load_to_sqlite()


if __name__ == "__main__":
    # For local testing: python src/loader.py
    run_loader()
