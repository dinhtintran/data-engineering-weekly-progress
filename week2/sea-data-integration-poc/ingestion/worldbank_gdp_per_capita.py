import json
import sys
from pathlib import Path
from typing import Dict, Any, Iterator, Optional

import logging
import requests


def fetch_worldbank_series(base_url: str, endpoint: str, fmt: str = "json", per_page: int = 1000, timeout: int = 30) -> Iterator[Dict[str, Any]]:
    """
    Stream all records for a World Bank indicator by requesting pages until exhausted.
    
    Make repeated API requests using the supplied base URL and endpoint, iterating through pagination and yielding each dictionary from the response "data" arrays.
    
    Parameters:
        base_url (str): Base API URL (for example, "http://api.worldbank.org/v2/").
        endpoint (str): Endpoint path for the desired indicator (for example, "country/USA/indicator/NY.GDP.PCAP.CD").
        fmt (str): Response format (commonly "json").
        per_page (int): Number of records to request per page.
        timeout (int): HTTP request timeout in seconds.
    
    Returns:
        Iterator[Dict[str, Any]]: Yields individual record dictionaries extracted from each page's "data" payload.
    """
    page = 1
    logger = logging.getLogger("ingestion.worldbank")
    while True:
        url = f"{base_url}{endpoint}?format={fmt}&per_page={per_page}&page={page}"
        logger.debug(f"Fetching page {page}: {url}")
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        payload = resp.json()
        if not isinstance(payload, list) or len(payload) < 2:
            break
        meta, data = payload[0], payload[1]
        if not data:
            break
        logger.info(f"Fetched page {page} with {len(data)} records")
        for item in data:
            yield item
        total_pages = meta.get("pages") or 1
        if page >= total_pages:
            break
        page += 1


def save_jsonl(items: Iterator[Dict[str, Any]], output_path: Path) -> int:
    """
    Write an iterator of dictionaries to a JSON Lines (JSONL) file.
    
    Ensures the output file's parent directory exists, opens the file with UTF-8 encoding, and writes each dictionary as a single JSON object on its own line.
    
    Parameters:
        items (Iterator[Dict[str, Any]]): An iterator yielding dictionaries to serialize; each item becomes one JSON line.
        output_path (Path): Destination path for the JSONL file; parent directories will be created if they do not exist.
    
    Returns:
        total (int): The number of records written to the file.
    """
    logger = logging.getLogger("ingestion.worldbank")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with output_path.open("w", encoding="utf-8") as f:
        for item in items:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
            count += 1
    logger.info(f"Saved {count} records to {output_path}")
    return count


def main(config: Dict[str, Any]) -> None:
    """
    Orchestrates ingestion: reads API and output settings from `config`, streams World Bank indicator data, and writes it to a JSON Lines file.
    
    Parameters:
        config (Dict[str, Any]): Configuration dictionary containing optional `api` and `output` sections.
            Expected keys in `api` (all optional; shown with defaults):
                - `base_url` (str): API root (default "https://api.worldbank.org/v2").
                - `endpoint` (str): Indicator endpoint path (default "/country/all/indicator/NY.GDP.PCAP.CD").
                - `format` (str): Response format (default "json").
                - `per_page` (int): Results per page (default 1000).
                - `timeout_sec` (int): HTTP request timeout seconds (default 30).
            Expected keys in `output`:
                - `raw_jsonl_path` (str): Filesystem path where the JSONL output will be written (default "data/raw/worldbank_gdp_per_capita.jsonl").
    """
    api_cfg = config.get("api", {})
    output_cfg = config.get("output", {})

    base_url = api_cfg.get("base_url", "https://api.worldbank.org/v2")
    endpoint = api_cfg.get("endpoint", "/country/all/indicator/NY.GDP.PCAP.CD")
    fmt = api_cfg.get("format", "json")
    per_page = int(api_cfg.get("per_page", 1000))
    timeout = int(api_cfg.get("timeout_sec", 30))

    raw_path = Path(output_cfg.get("raw_jsonl_path", "data/raw/worldbank_gdp_per_capita.jsonl"))

    logger = logging.getLogger("ingestion.worldbank")
    logger.info("Starting ingestion from World Bank API")
    items = fetch_worldbank_series(base_url, endpoint, fmt=fmt, per_page=per_page, timeout=timeout)
    total = save_jsonl(items, raw_path)
    logger.info(f"Ingestion completed: {total} records")


if __name__ == "__main__":
    # Minimal inline config fallback for ad-hoc runs
    cfg: Dict[str, Any] = {
        "api": {
            "base_url": "https://api.worldbank.org/v2",
            "endpoint": "/country/all/indicator/NY.GDP.PCAP.CD",
            "format": "json",
            "per_page": 1000,
            "timeout_sec": 30,
        },
        "output": {
            "raw_jsonl_path": "data/raw/worldbank_gdp_per_capita.jsonl",
        },
    }
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
    main(cfg)