import json
import time
import logging
import argparse
import os
from apify_client import ApifyClient
from dotenv import load_dotenv
from config_loader import load_config, get_path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawl_places.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

# Load configuration
config = load_config()
APIFY_TOKEN = os.getenv("APIFY_TOKEN")
ACTOR_ID = get_path(config, 'apify', 'actor_id', default='compass/crawler-google-places')

def crawl_raw(query, save_path="../data/raw/raw_places.json", max_crawled_places=25, max_reviews=5):
    """
    Crawl Google Places data using Apify API
    
    Args:
        query (str): Search query for places (e.g., 'billiard, Ho Chi Minh City')
        save_path (str): Path to save raw JSON data
        max_crawled_places (int): Maximum number of places to crawl (default: 25)
        max_reviews (int): Maximum number of reviews per place (default: 5)
    """
    if not APIFY_TOKEN:
        logger.error("APIFY_TOKEN not found in environment variables!")
        raise ValueError("Please set APIFY_TOKEN in .env file")
    
    logger.info(f"Initializing Apify client for query: {query}")
    client = ApifyClient(APIFY_TOKEN)

    # Get config values
    scrape_detail = get_path(config, 'apify', 'scrape_place_detail_page', default=False)
    reviews_sort = get_path(config, 'apify', 'reviews_sort', default='newest')
    
    run_input = {
        "searchStringsArray": [query], 
        "maxCrawledPlaces": max_crawled_places,
        "maxReviews": max_reviews,
        "scrapePlaceDetailPage": scrape_detail,
        "reviewsSort": reviews_sort
    }

    logger.info(f"Running Apify Google Maps Scraper with query: {query}")
    try:
        run = client.actor(ACTOR_ID).call(run_input=run_input)
        logger.info(f"Apify run completed. Run ID: {run.get('id')}")
    except Exception as e:
        logger.error(f"Error calling Apify actor: {str(e)}")
        raise

    # Get dataset
    dataset_id = run["defaultDatasetId"]
    logger.info(f"Fetching dataset: {dataset_id}")
    items = client.dataset(dataset_id).list_items().items
    logger.info(f"Retrieved {len(items)} places from Apify")

    # Transform (Google Places API format)
    data = []
    for place in items:
        detail = {
            "place_id": place.get("placeId"),
            "name": place.get("title"),
            "rating": place.get("totalScore"),
            "user_ratings_total": place.get("reviewsCount"),
            "geometry": place.get("placeLocation") or place.get("location"),
            "address": place.get("address"),
            "types": place.get("categories"),
            "reviews": []
        }
        for rev in place.get("reviews", []):
            detail["reviews"].append({
                "author_name": rev.get("name"),
                "rating": rev.get("stars"),
                "text": rev.get("text"),
                "time": rev.get("publishedAtDate")
            })
        data.append(detail)

    # Make new folder if does not exist
    os.makedirs(os.path.dirname(save_path), exist_ok=True)

    with open(save_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    logger.info(f"Successfully saved {len(data)} places to {save_path}")
    return data

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Crawl Google Places data using Apify API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python crawl_places.py "billiard, Ho Chi Minh City"
  python crawl_places.py "coffee shop, New York" --max-crawled-places 50 --max-reviews 10
  python crawl_places.py  # Will prompt for query input
        """
    )
    parser.add_argument(
        "query",
        type=str,
        nargs='?',
        help="Search query for places (e.g., 'coffee shop, New York'). If not provided, will prompt for input."
    )
    # Get defaults from config
    default_max_places = get_path(config, 'apify', 'default_max_places', default=25)
    default_max_reviews = get_path(config, 'apify', 'default_max_reviews', default=5)
    raw_data_dir = get_path(config, 'paths', 'raw_data_dir', default='../data/raw')
    default_raw_json = get_path(config, 'paths', 'default_raw_json', default='raw_places.json')
    default_output = os.path.join(raw_data_dir, default_raw_json)
    
    parser.add_argument(
        "--max-crawled-places",
        type=int,
        default=default_max_places,
        help=f"Maximum number of places to crawl (default: {default_max_places} from config)"
    )
    parser.add_argument(
        "--max-reviews",
        type=int,
        default=default_max_reviews,
        help=f"Maximum number of reviews per place (default: {default_max_reviews} from config)"
    )
    parser.add_argument(
        "--output",
        type=str,
        default=default_output,
        help=f"Path to save raw JSON data (default: {default_output} from config)"
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to config.yaml file (default: ../config.yaml)"
    )
    
    args = parser.parse_args()
    
    # Reload config if custom path provided
    if args.config:
        config = load_config(args.config)
    
    # If query is not provided, prompt user for input
    query = args.query
    if not query:
        query = input("Type keywords to search the place (e.g. 'coffee shop, New York'): ")
    
    logger.info(f"Starting crawl with query='{query}', max_crawled_places={args.max_crawled_places}, max_reviews={args.max_reviews}")
    crawl_raw(
        query=query,
        save_path=args.output,
        max_crawled_places=args.max_crawled_places,
        max_reviews=args.max_reviews
    )
