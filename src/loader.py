import logging
import sqlite3
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def load_to_sqlite(
    input_path: str = "/workspaces/CIS-2/data/clean_products.csv",
    db_path: str = "/workspaces/CIS-2/data/output.db",
    table_name: str = "products",
) -> None:
    """
    Load cleaned Sulpak products into a SQLite DB.

    - Reads cleaned CSV
    - Creates data folder if needed
    - Writes/overwrites the table in SQLite
    """
    input_file = Path(input_path)
    df = pd.read_csv(input_file)
    db_file = Path(db_path)
    db_file.parent.mkdir(parents=True, exist_ok=True)
    conn: Optional[sqlite3.Connection] = None
    try:
        conn = sqlite3.connect(db_file)
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        logger.info(
            "Inserted ",
            len(df),
            table_name,
            db_file,
        )
    finally:
        if conn is not None:
            conn.close()


def run_loader(
    input_path: str = "/workspaces/CIS-2/data/clean_products.csv",
    db_path: str = "/workspaces/CIS-2/data/output.db",
    table_name: str = "products",
) -> None:
    load_to_sqlite(
        input_path=input_path,
        db_path=db_path,
        table_name=table_name,
    )


if __name__ == "__main__":
    run_loader()
