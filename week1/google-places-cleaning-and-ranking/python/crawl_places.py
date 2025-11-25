import requests
import json
import time


API_KEY = ""

def text_search(query):
    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    params = {"query": query, "key": API_KEY}
    res = requests.get(url, params=params).json()
    return res.get("results", [])

def get_place_details(place_id):
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        "place_id": place_id,
        "key": API_KEY,
        "fields": "place_id,name,rating,user_ratings_total,geometry,formatted_address,types,reviews"
    }
    res = requests.get(url, params=params).json()
    return res.get("result", {})

def crawl(query, save_path="../data/raw/raw_places.json"):
    print(f"Searching for: {query}")
    results = text_search(query)

    data = []
    for place in results:
        pid = place["place_id"]
        print("Fetching details for: ", pid)
        detail = get_place_details(pid)
        data.append(detail)
        time.sleep(0.5)

    with open(save_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Saved to {save_path}")

if __name__ == "__main__":
    query = input("Type keywords to search the place: ")
    crawl(query)