# Sulpak Smartphone Data Pipeline

## Project Overview

This project uses Apache Airflow to automate the process of gathering product information from Sulpak.kz, cleansing the collected data, and storing it in a SQLite database.

The pipeline is automated with Apache Airflow and includes:

1. Web scraping of JavaScript-rendered, paginated, dynamic websites

2. Cleaning & preprocessing the collected data

3. SQLite3 database storage

4. Automated scheduling using an Airflow DAG 

## Website Selected

[Sulpak.kz](https://www.sulpak.kz/f/smartfoniy)

The website fulfills the course constraints:

1. dynamic content

2. pagination rendered with JavaScript

3. product cards appear after client-side rendering

4. requires Selenium to extract structured data

## Project Structure

```
<repo>/
├─ dags/
│  └─ airflow_dag.py
│
├─ src/
│  ├─ scraper.py
│  ├─ cleaner.py
│  └─ loader.py
│
├─ data/
│  ├─ raw_products.csv
│  ├─ clean_products.csv
│  └─ output.db
│
├─ docker-compose.yaml
├─ requirements.txt
├─ .venv/
└─ README.md
```

## Pipeline Components

### 1.Scraper (scraper.py)
Scrapes all smartphone product pages from Sulpak using:

- Selenium WebDriver
- dynamic “Next page” navigation
- randomized delays (10–30 seconds) to avoid blocking
- attribute-based extraction (data-name, data-price)
- product URL parsing

Command-line example:
```python
python src/scraper.py
```
Output:
```
data/raw_products.csv
```
### 2.Cleaner (cleaner.py)
Cleaning steps include:
- removing duplicates
- removing rows with missing critical fields
- converting prices to numeric
- cleaning rating & review count when available
- keeping only useful columns
- validating that final dataset ≥ 200 rows

Run manually:
```python
python src/cleaner.py
```
Output:
```
data/clean_products.csv
```

### 3.Loader (loader.py)
Loads cleaned data into SQLite:
- creates database if not exists
- writes table products
- replaces old data each run

Run manually:
```python
python src/loader.py
```
Output:
```
data/output.db
```
## SQLite Storage
The loader creates a SQLite database:
```python
data/output.db
```
Table structure used by the project:
```sql
CREATE TABLE products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    price REAL,
    product_url TEXT
); 
```


## Airflow Automation
Apache Airflow is used to orchestrate and schedule the entire ETL workflow:
```
scrape_sulpak → clean_products → load_to_sqlite
```
### The project uses a Docker-based Airflow environment that includes:
- Airflow Webserver — web interface for monitoring DAGs
- Airflow Scheduler — executes tasks according to the schedule
- PostgreSQL — Airflow metadata database
- Initialization service — creates admin user and initializes the DB

Initialize Airflow:
```python
docker-compose up airflow-init
```
Start all services in the background:
```python
docker-compose up -d
```
## Accessing the Airflow Web UI

Airflow UI:

[http://localhost:8080](http://localhost:8080)

Login:
```
admin / admin
```

Enable the DAG:
```
sulpak_pipeline
```
## DAG Configuration
The DAG is defined in:
```python
dags/airflow_dag.py
```
Key parameters:
```python
schedule="@daily"
catchup=False
retries=2
retry_delay=timedelta(minutes=5)
```
This ensures that the entire ETL process runs automatically once every 24 hours, with retry logic to recover from temporary failures.
