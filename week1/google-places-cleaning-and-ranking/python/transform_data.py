import json
import os
import pandas as pd 

RAW_JSON_PATH = "../data/raw/billiard_raw_places.json"

if not os.path.exists(RAW_JSON_PATH):
    raise FileNotFoundError(f"{RAW_JSON_PATH} does not exist!")

with open(RAW_JSON_PATH, "r", encoding="utf-8") as f:
    raw_data = json.load(f)

df = pd.DataFrame(raw_data)

df['user_rating_total'] = df['user_ratings_total'].fillna(0)
df['rating'] = df['rating'].fillna(0)

df['latitude'] = df['geometry'].apply(lambda x: x.get('lat') if isinstance(x, dict) else None)
df['longtitude'] = df['geometry'].apply(lambda x: x.get('lng') if isinstance(x, dict) else None)

OUTPUT_CSV_PATH = "../data/clean/billiard_clean_place.csv"
columns_to_save = ['place_id', 'name', 'rating', 'user_ratings_total', 'latitude', 'longtitude', 'address', 'types']

os.makedirs(os.path.dirname(OUTPUT_CSV_PATH) or '.', exist_ok=True)

df[columns_to_save].to_csv(OUTPUT_CSV_PATH, index=False, encoding="utf-8-sig")
print(f"Saved cleaned CSV to {OUTPUT_CSV_PATH}")