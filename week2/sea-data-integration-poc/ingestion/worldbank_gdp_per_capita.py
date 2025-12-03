import json
import sys
from pathlib import Path
from typing import Dict, Any, Iterator, Optional

import logging
import requests


def fetch_worldbank_series(base_url: str, endpoint: str, fmt: str = "json", per_page: int = 1000, timeout: int = 30) -> Iterator[Dict[str, Any]]:
    """
    Stream all records from the World Bank API for a given indicator, handling pagination.
    Yields raw item dicts from the "data" section.
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
