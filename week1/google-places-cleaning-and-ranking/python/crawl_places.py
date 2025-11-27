import json
import time
import logging
import argparse
from apify_client import ApifyClient
import os
from dotenv import load_dotenv

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
APIFY_TOKEN = os.getenv("APIFY_TOKEN")
ACTOR_ID = 'compass/crawler-google-places'  # Google Maps Scraper

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

    run_input = {
        "searchStringsArray": [query], 
        "maxCrawledPlaces": max_crawled_places,
        "maxReviews": max_reviews,
        "scrapePlaceDetailPage": False,
        "reviewsSort": "newest"
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
    parser.add_argument(
        "--max-crawled-places",
        type=int,
        default=25,
        help="Maximum number of places to crawl (default: 25)"
    )
    parser.add_argument(
        "--max-reviews",
        type=int,
        default=5,
        help="Maximum number of reviews per place (default: 5)"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="../data/raw/raw_places.json",
        help="Path to save raw JSON data (default: ../data/raw/raw_places.json)"
    )
    
    args = parser.parse_args()
    
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
