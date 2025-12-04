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
    - Basic cleaning of rating / review_count
    - Ensure at least `min_records` rows
    """
    input_file = Path(input_path)
    if not input_file.exists():
        raise FileNotFoundError(f"Raw data file not found: {input_file}")

    df = pd.read_csv(input_file)
    logger.info("Loaded %d raw rows from %s", len(df), input_file)

    # Drop rows with empty critical fields
    df = df.dropna(subset=["name", "price_clean"])

    # Normalize name: strip whitespace
    df["name"] = df["name"].astype(str).str.strip()

    # Convert price_clean → price (float)
    df["price_clean"] = df["price_clean"].astype(str).str.replace(r"[^\d.]", "", regex=True)
    df["price"] = pd.to_numeric(df["price_clean"], errors="coerce")

    # Basic rating cleaning
    if "rating_raw" in df.columns:
        df["rating"] = pd.to_numeric(df["rating_raw"], errors="coerce")

    # Reviews count: extract digits if any
    if "reviews_raw" in df.columns:
        df["reviews_count"] = (
            df["reviews_raw"]
            .astype(str)
            .str.extract(r"(\d+)", expand=False)
            .astype("float")
        )

    # Drop duplicates by (name, product_url) if columns exist
    subset_cols = [col for col in ["name", "product_url"] if col in df.columns]
    if subset_cols:
        before = len(df)
        df = df.drop_duplicates(subset=subset_cols)
        logger.info("Dropped %d duplicates", before - len(df))

    # Keep only useful columns for DB
    keep_cols = ["name", "price", "product_url"]
    if "rating" in df.columns:
        keep_cols.append("rating")
    if "reviews_count" in df.columns:
        keep_cols.append("reviews_count")

    df = df[keep_cols]

    # Drop rows with missing price
    df = df.dropna(subset=["price"])

    if len(df) < min_records:
        raise ValueError(
            f"Not enough records after cleaning: {len(df)} (< {min_records})"
        )

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    logger.info("Saved %d cleaned rows to %s", len(df), output_path)


def run_cleaner() -> None:
    """Entry point for Airflow / CLI."""
    clean_products()


if __name__ == "__main__":
    # For local testing: python src/cleaner.py
    run_cleaner()
