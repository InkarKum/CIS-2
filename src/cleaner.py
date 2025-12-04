import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def clean_products(
    input_path: str = "../data/raw_products.csv",
    output_path: str = "../data/clean_products.csv",
    min_records: int = 20,
) -> None:
    """
    Clean raw Sulpak products and save cleaned dataset.

    Steps:
    - Drop rows without name or price
    - Convert price to numeric
    - Normalize text
    - Drop duplicates
    """
    input_file = Path(input_path)

    df = pd.read_csv(input_file)
    logger.info("Loaded %d raw rows from %s", len(df), input_file)

    df = df.dropna(subset=["name", "price_clean"])
    df["name"] = df["name"].astype(str).str.strip()
    df["price_clean"] = df["price_clean"].astype(str).str.replace(r"[^\d.]", "", regex=True)
    df["price"] = pd.to_numeric(df["price_clean"], errors="coerce")

    subset_cols = [col for col in ["name", "product_url"] if col in df.columns]
    if subset_cols:
        before = len(df)
        df = df.drop_duplicates(subset=subset_cols)
        logger.info("Dropped %d duplicates", before - len(df))

    keep_cols = ["name", "price", "product_url"]
    df = df[keep_cols]
    df = df.dropna(subset=["price"])

    if len(df) < min_records:
        raise ValueError(
            f"Not enough records after cleaning: {len(df)} (< {min_records})"
        )

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    logger.info("Saved %d cleaned rows to %s", len(df), output_path)


def run_cleaner(
    input_path: str = "/workspaces/CIS-2/data/raw_products.csv",
    output_path: str = "/workspaces/CIS-2/data/clean_products.csv",
    min_records: int = 20,
) -> None:
    clean_products(
        input_path=input_path,
        output_path=output_path,
        min_records=min_records,
    )


if __name__ == "__main__":
    run_cleaner()