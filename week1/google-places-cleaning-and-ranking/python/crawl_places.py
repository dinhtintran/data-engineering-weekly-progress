import json
import time
from apify_client import ApifyClient
import os
from dotenv import load_dotenv

load_dotenv()
APIFY_TOKEN = os.getenv("APIFY_TOKEN")
ACTOR_ID = 'compass/crawler-google-places'  # Google Maps Scraper

def crawl_raw(query, save_path="../data/raw/raw_places.json"):
    client = ApifyClient(APIFY_TOKEN)

    run_input = {
        "searchStringsArray": [query], 
        "maxCrawledPlaces": 25,
        "maxReviews": 5,
        "scrapePlaceDetailPage": False,
        "reviewsSort": "newest"
    }

    print(f"Running Apify Google Maps Scraper with query: {query}")
    run = client.actor(ACTOR_ID).call(run_input=run_input)

    # Get dataset
    dataset_id = run["defaultDatasetId"]
    items = client.dataset(dataset_id).list_items().items

    # Tranfer (Google Places API)
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
    import os
    os.makedirs(os.path.dirname(save_path), exist_ok=True)

    with open(save_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Saved raw data to {save_path}")

if __name__ == "__main__":
    query = input("Type keywords to search the place (e.g. 'coffee shop, New York'): ")
    crawl_raw(query)
