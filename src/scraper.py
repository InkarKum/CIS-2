import time
import logging
import random
from pathlib import Path
from typing import List, Dict

import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def random_delay(a: int = 5, b: int = 12):
    delay = random.randint(a, b)
    logger.info("Sleeping for %d seconds...", delay)
    time.sleep(delay)


def _extract_products_from_page(page) -> List[Dict]:
    cards = page.locator(
        "#products div.product__item.product__item-js.tile-container"
    )

    count = cards.count()
    results = []

    for i in range(count):
        card = cards.nth(i)
        try:
            name = (card.get_attribute("data-name") or "").strip()
            if not name:
                continue

            price_raw = (card.get_attribute("data-price") or "").strip()
            price_clean = (
                price_raw.replace("\u00a0", " ").replace("₸", "").replace(" ", "").strip()
            )

            try:
                link = card.locator("a").first.get_attribute("href")
            except:
                link = None

            results.append(
                {
                    "name": name,
                    "price_raw": price_raw,
                    "price_clean": price_clean,
                    "product_url": link,
                }
            )
        except Exception as e:
            logger.warning("Parse error: %s", e)
            continue

    logger.info("Parsed %d products", len(results))
    return results


def scrape_sulpak_category(
    category_url: str,
    max_pages: int = 50,
    wait_timeout: int = 10_000,
) -> pd.DataFrame:

    all_items = []
    visited = set()
    current = 1

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1920, "height": 1080})

        logger.info("Opening %s", category_url)
        try:
            page.goto(category_url, wait_until="domcontentloaded", timeout=60_000)
        except PlaywrightTimeoutError:
            logger.warning("Initial load timeout")

        visited.add(page.url)

        while current <= 15:  # LIMIT PAGE COUNT TO 15
            logger.info("Processing page %d → %s", current, page.url)

            try:
                page.wait_for_selector(
                    "#products div.product__item.product__item-js.tile-container",
                    timeout=wait_timeout,
                )
            except PlaywrightTimeoutError:
                logger.warning("Timeout waiting for products")
                break

            # Scroll for lazy loading
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            random_delay()

            all_items.extend(_extract_products_from_page(page))

            # NEXT button
            next_btn = page.locator("li.pagination__next a[data-url]")
            if next_btn.count() == 0:
                logger.info("No NEXT → last page.")
                break

            next_rel = (next_btn.first.get_attribute("data-url") or "").strip()
            if not next_rel:
                break

            next_url = "https://www.sulpak.kz" + next_rel

            if next_url in visited:
                logger.info("Already visited → stop.")
                break

            visited.add(next_url)
            current += 1

            logger.info("Navigating to: %s", next_url)
            try:
                page.goto(next_url, wait_until="domcontentloaded", timeout=60_000)
            except PlaywrightTimeoutError:
                logger.warning("Timeout on page %d", current)
                break

            random_delay()

        browser.close()

    df = pd.DataFrame(all_items)
    if not df.empty:
        df = df.drop_duplicates(subset=["name", "product_url"])

    logger.info("Total scraped: %d items", len(df))
    return df


def run_scraper(category_url: str, output_path: str):
    output_dir = Path(output_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)

    df = scrape_sulpak_category(category_url)
    df.to_csv(output_path, index=False)

    logger.info("Saved scraped data to %s", output_path)
