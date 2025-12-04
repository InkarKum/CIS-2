import time
import logging
import random
from pathlib import Path
from typing import List, Dict
import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from webdriver_manager.chrome import ChromeDriverManager

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def random_delay(a: int = 10, b: int = 30):
    """Sleep a random number of seconds between a and b."""
    delay = random.randint(a, b)
    logger.info(f"Sleeping for {delay} seconds...")
    time.sleep(delay)


def _init_driver(headless: bool = True) -> webdriver.Chrome:
    """Create and return a configured Chrome WebDriver."""
    options = Options()
    if headless:
        options.add_argument("--headless=new")

    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    return driver


def _extract_products_from_page(driver: webdriver.Chrome) -> List[Dict]:
    """Parse all product cards from the current page."""
    product_cards = driver.find_elements(
        By.CSS_SELECTOR,
        "#products div.product__item.product__item-js.tile-container",
    )

    records: List[Dict] = []

    for card in product_cards:
        try:
            name = (card.get_attribute("data-name") or "").strip()
            if not name:
                continue

            try:
                link_el = card.find_element(By.CSS_SELECTOR, "a")
                product_url = link_el.get_attribute("href")
            except Exception:
                product_url = None

            price_raw = (card.get_attribute("data-price") or "").strip()
            price_clean = (
                price_raw.replace("\u00a0", " ")
                .replace("₸", "")
                .replace(" ", "")
                .strip()
            )

            rating = None
            reviews_count = None

            try:
                rating_el = card.find_element(By.CSS_SELECTOR, ".product__rating")
                rating = rating_el.get_attribute("data-rating") or rating_el.text.strip()
            except Exception:
                pass

            try:
                reviews_el = card.find_element(By.CSS_SELECTOR, ".product__reviews")
                reviews_count = reviews_el.text.strip()
            except Exception:
                pass

            records.append(
                {
                    "name": name,
                    "price_raw": price_raw,
                    "price_clean": price_clean,
                    "rating_raw": rating,
                    "reviews_raw": reviews_count,
                    "product_url": product_url,
                }
            )

        except Exception as e:
            logger.warning("Error while parsing card: %s", e)
            continue

    logger.info("Parsed %d products on this page", len(records))
    return records


def scrape_sulpak_category(
    category_url: str,
    max_pages: int = 50,
    wait_timeout: int = 10,
) -> pd.DataFrame:
    """Scrape ALL Sulpak pages by clicking NEXT until it disappears."""
    driver = _init_driver(headless=True)
    driver.get(category_url)

    all_records: List[Dict] = []
    current_page = 1
    seen_urls = set()

    while current_page <= max_pages:
        logger.info(f"Processing page {current_page}")

        try:
            WebDriverWait(driver, wait_timeout).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "#products div.product__item.product__item-js.tile-container")
                )
            )
        except TimeoutException:
            logger.warning(f"Timeout waiting for product container on page {current_page}")
            break

        # Scroll + random delay
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        random_delay()  # 10–30 seconds

        all_records.extend(_extract_products_from_page(driver))

        # NEXT button
        try:
            next_el = driver.find_element(
                By.CSS_SELECTOR,
                "li.pagination__next a[data-url]",
            )
        except NoSuchElementException:
            logger.info("No NEXT link found → last page.")
            break

        next_rel_url = (next_el.get_attribute("data-url") or "").strip()
        if not next_rel_url:
            logger.info("NEXT link has empty data-url → stop.")
            break

        next_full_url = "https://www.sulpak.kz" + next_rel_url

        if next_full_url in seen_urls:
            logger.info(f"Already visited {next_full_url} → stopping loop.")
            break

        seen_urls.add(next_full_url)
        current_page += 1

        logger.info(f"Going to next page: {next_full_url}")
        driver.get(next_full_url)

        random_delay()  # delay before loading next page

    driver.quit()

    df = pd.DataFrame(all_records)
    if not df.empty and {"name", "product_url"} <= set(df.columns):
        df = df.drop_duplicates(subset=["name", "product_url"])

    logger.info(f"Total scraped products: {len(df)}")
    return df


def run_scraper(
    category_url: str = "https://www.sulpak.kz/f/smartfoniy",
    output_path: str = "data/raw_products.csv",
) -> None:
    Path("data").mkdir(parents=True, exist_ok=True)
    df = scrape_sulpak_category(category_url=category_url)
    df.to_csv(output_path, index=False)
    logger.info(f"Saved raw products to {output_path}")


if __name__ == "__main__":
    run_scraper()
