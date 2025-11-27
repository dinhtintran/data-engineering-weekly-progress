#!/usr/bin/env python3
"""
End-to-end pipeline runner for Google Places ETL project.

Steps:
1. Crawl data from Apify Google Places actor
2. Transform raw JSON to clean CSV
3. Load into SQLite and generate ranked CSV
"""
import argparse
import logging
import os
import sqlite3
from typing import List, Optional, Tuple

import pandas as pd

from config_loader import load_config, get_path
import crawl_places
import transform_data as transformer


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ],
)
logger = logging.getLogger(__name__)


def resolve_file_path(
    base_dir: str,
    explicit_path: Optional[str],
    name_arg: Optional[str],
    default_name: str,
) -> str:
    """
    Resolve file path based on precedence:
    1. explicit_path (absolute or relative)
    2. name_arg joined with base_dir
    3. default_name joined with base_dir
    """
    if explicit_path:
        return explicit_path

    file_name = name_arg or default_name
    if os.path.isabs(file_name):
        return file_name

    return os.path.join(base_dir, file_name)


def load_and_rank(clean_csv_path: str, db_path: str, ranked_csv_path: str) -> Tuple[str, str]:
    """Load clean CSV into SQLite, run ranking query, export ranked CSV."""
    logger.info("Loading cleaned data from %s", clean_csv_path)
    if not os.path.exists(clean_csv_path):
        raise FileNotFoundError(f"Clean CSV not found: {clean_csv_path}")

    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    os.makedirs(os.path.dirname(ranked_csv_path) or ".", exist_ok=True)

    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_csv(clean_csv_path)
        logger.info("Writing %d rows to SQLite database %s", len(df), db_path)
        df.to_sql('places', conn, if_exists='replace', index=False)

        conn.execute("ALTER TABLE places ADD COLUMN main_type TEXT;")
        conn.execute("""
            UPDATE places
            SET main_type = TRIM(
                REPLACE(
                    REPLACE(
                        REPLACE(types, '[', ''),
                        ']', ''),
                    '"', '')
            )
            WHERE main_type IS NULL OR main_type = '';
        """)

        logger.info("Creating place_ranking table with dense rank")
        conn.execute("DROP TABLE IF EXISTS place_ranking;")
        conn.execute("""
            CREATE TABLE place_ranking AS
            SELECT
                place_id,
                name,
                rating,
                user_ratings_total,
                COALESCE(main_type, types) AS category,
                DENSE_RANK() OVER (
                    PARTITION BY COALESCE(main_type, types)
                    ORDER BY rating DESC, user_ratings_total DESC
                ) AS rating_rank
            FROM places;
        """)

        logger.info("Exporting ranked results to %s", ranked_csv_path)
        ranked_df = pd.read_sql_query("""
            SELECT
                place_id,
                name,
                rating,
                user_ratings_total,
                category,
                rating_rank
            FROM place_ranking
            ORDER BY category, rating_rank;
        """, conn)
        ranked_df.to_csv(ranked_csv_path, index=False, encoding="utf-8-sig")

        conn.commit()
    finally:
        conn.close()

    return db_path, ranked_csv_path


def parse_columns(columns_str: Optional[str]) -> Optional[List[str]]:
    if not columns_str:
        return None
    return [col.strip() for col in columns_str.split(",") if col.strip()]


def main():
    parser = argparse.ArgumentParser(
        description="Run the full Google Places ETL pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full pipeline with defaults from config.yaml
  python pipeline.py --query "billiard, Ho Chi Minh City"

  # Override output file names
  python pipeline.py --query "coffee shop, Hanoi" --raw-output-name coffee_raw.json --clean-output-name coffee_clean.csv

  # Use existing raw file, skip crawling, only transform + load
  python pipeline.py --skip-crawl --raw-path ../data/raw/existing.json
        """
    )
    parser.add_argument("--query", type=str, help="Search query for Apify crawler")
    parser.add_argument("--max-crawled-places", type=int, default=None, help="Max places to crawl (override config)")
    parser.add_argument("--max-reviews", type=int, default=None, help="Max reviews per place (override config)")

    parser.add_argument("--raw-path", type=str, default=None, help="Explicit path to raw JSON file")
    parser.add_argument("--raw-output-name", type=str, default=None, help="File name for raw JSON (stored in raw data dir)")

    parser.add_argument("--clean-path", type=str, default=None, help="Explicit path to cleaned CSV")
    parser.add_argument("--clean-output-name", type=str, default=None, help="File name for cleaned CSV (stored in clean data dir)")

    parser.add_argument("--database-path", type=str, default=None, help="Explicit path to SQLite database file")
    parser.add_argument("--database-name", type=str, default=None, help="File name for SQLite database (stored in output dir)")

    parser.add_argument("--ranked-path", type=str, default=None, help="Explicit path to ranked CSV output")
    parser.add_argument("--ranked-output-name", type=str, default=None, help="File name for ranked CSV (stored in output dir)")

    parser.add_argument("--columns", type=str, default=None, help="Comma-separated list of columns to save (override config)")
    parser.add_argument("--default-rating", type=float, default=None, help="Value to fill missing ratings (override config)")
    parser.add_argument("--default-reviews", type=int, default=None, help="Value to fill missing review counts (override config)")

    parser.add_argument("--config", type=str, default=None, help="Path to config.yaml file (default: ../config.yaml)")
    parser.add_argument("--skip-crawl", action="store_true", help="Skip crawling step")
    parser.add_argument("--skip-transform", action="store_true", help="Skip transform step")
    parser.add_argument("--skip-load", action="store_true", help="Skip load + ranking step")

    args = parser.parse_args()

    if not args.skip_crawl and not args.query:
        parser.error("--query is required unless --skip-crawl is specified")

    config = load_config(args.config)
    crawl_places.config = config
    transformer.config = config

    raw_dir = get_path(config, 'paths', 'raw_data_dir', default='../data/raw')
    clean_dir = get_path(config, 'paths', 'clean_data_dir', default='../data/clean')
    output_dir = get_path(config, 'paths', 'output_dir', default='../output')

    default_raw_name = get_path(config, 'paths', 'default_raw_json', default='raw_places.json')
    default_clean_name = get_path(config, 'paths', 'default_clean_csv', default='clean_places.csv')
    default_db_name = get_path(config, 'paths', 'default_database', default='places.db')
    default_ranked_name = get_path(config, 'paths', 'default_ranked_csv', default='ranked_places.csv')

    raw_path = resolve_file_path(raw_dir, args.raw_path, args.raw_output_name, default_raw_name)
    clean_path = resolve_file_path(clean_dir, args.clean_path, args.clean_output_name, default_clean_name)
    db_path = resolve_file_path(output_dir, args.database_path, args.database_name, default_db_name)
    ranked_path = resolve_file_path(output_dir, args.ranked_path, args.ranked_output_name, default_ranked_name)

    logger.info("Pipeline settings:")
    logger.info("  Raw JSON path: %s", raw_path)
    logger.info("  Clean CSV path: %s", clean_path)
    logger.info("  Database path: %s", db_path)
    logger.info("  Ranked CSV path: %s", ranked_path)

    if not args.skip_crawl:
        max_places = args.max_crawled_places or get_path(config, 'apify', 'default_max_places', default=25)
        max_reviews = args.max_reviews or get_path(config, 'apify', 'default_max_reviews', default=5)

        logger.info("Step 1/3: Crawling places for query '%s'", args.query)
        crawl_places.crawl_raw(
            query=args.query,
            save_path=raw_path,
            max_crawled_places=max_places,
            max_reviews=max_reviews
        )
    else:
        logger.info("Skipping crawl step")

    if not args.skip_transform:
        logger.info("Step 2/3: Transforming raw data")
        columns_override = parse_columns(args.columns)
        transformer.transform_data(
            raw_json_path=raw_path,
            output_csv_path=clean_path,
            output_file_name=None,  # not used since output_csv_path provided
            columns_to_save_override=columns_override,
            default_rating_override=args.default_rating,
            default_reviews_override=args.default_reviews,
        )
    else:
        logger.info("Skipping transform step")

    if not args.skip_load:
        logger.info("Step 3/3: Loading into SQLite and ranking")
        load_and_rank(clean_path, db_path, ranked_path)
    else:
        logger.info("Skipping load/ranking step")

    logger.info("Pipeline completed successfully")
    logger.info("Outputs:")
    logger.info("  Raw JSON: %s", raw_path)
    logger.info("  Clean CSV: %s", clean_path)
    if not args.skip_load:
        logger.info("  SQLite DB: %s", db_path)
        logger.info("  Ranked CSV: %s", ranked_path)


if __name__ == "__main__":
    main()

