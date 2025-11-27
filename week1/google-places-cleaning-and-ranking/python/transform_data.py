import json
import os
import pandas as pd
import logging
import argparse
from config_loader import load_config, get_path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('transform_data.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load configuration
config = load_config()

def transform_data(raw_json_path=None, output_csv_path=None):
    """
    Transform raw JSON data to cleaned CSV format
    
    Args:
        raw_json_path (str): Path to raw JSON file. If None, uses config default.
        output_csv_path (str): Path to save cleaned CSV file. If None, uses config default.
    """
    # Get defaults from config if not provided
    if raw_json_path is None:
        raw_data_dir = get_path(config, 'paths', 'raw_data_dir', default='../data/raw')
        default_raw_json = get_path(config, 'paths', 'default_raw_json', default='raw_places.json')
        raw_json_path = os.path.join(raw_data_dir, default_raw_json)
    
    if output_csv_path is None:
        clean_data_dir = get_path(config, 'paths', 'clean_data_dir', default='../data/clean')
        default_clean_csv = get_path(config, 'paths', 'default_clean_csv', default='clean_places.csv')
        output_csv_path = os.path.join(clean_data_dir, default_clean_csv)
    
    logger.info(f"Starting data transformation from {raw_json_path}")
    
    if not os.path.exists(raw_json_path):
        logger.error(f"Raw data file not found: {raw_json_path}")
        raise FileNotFoundError(f"{raw_json_path} does not exist!")

    logger.info("Loading raw JSON data...")
    with open(raw_json_path, "r", encoding="utf-8") as f:
        raw_data = json.load(f)
    
    logger.info(f"Loaded {len(raw_data)} records from JSON")

    df = pd.DataFrame(raw_data)
    logger.info(f"Created DataFrame with shape: {df.shape}")

    # Handle missing values
    logger.info("Handling missing values...")
    default_rating = get_path(config, 'processing', 'default_rating', default=0)
    default_reviews = get_path(config, 'processing', 'default_user_ratings_total', default=0)
    
    df['user_ratings_total'] = df['user_ratings_total'].fillna(default_reviews)
    df['rating'] = df['rating'].fillna(default_rating)
    
    missing_ratings = df['rating'].isna().sum()
    missing_reviews = df['user_ratings_total'].isna().sum()
    if missing_ratings > 0 or missing_reviews > 0:
        logger.warning(f"Filled {missing_ratings} missing ratings and {missing_reviews} missing review counts with 0")

    # Extract latitude and longitude from geometry
    logger.info("Extracting coordinates from geometry...")
    df['latitude'] = df['geometry'].apply(lambda x: x.get('lat') if isinstance(x, dict) else None)
    df['longitude'] = df['geometry'].apply(lambda x: x.get('lng') if isinstance(x, dict) else None)
    
    # Fix typo in column name (longtitude -> longitude)
    if 'longtitude' in df.columns:
        df['longitude'] = df['longtitude']
        df = df.drop(columns=['longtitude'])
    
    coordinates_extracted = df['latitude'].notna().sum()
    logger.info(f"Extracted coordinates for {coordinates_extracted} places")

    # Get columns to save from config
    columns_to_save = get_path(config, 'processing', 'columns_to_save', default=[
        'place_id', 'name', 'rating', 'user_ratings_total', 
        'latitude', 'longitude', 'address', 'types'
    ])
    
    # Check if all columns exist
    missing_cols = [col for col in columns_to_save if col not in df.columns]
    if missing_cols:
        logger.warning(f"Missing columns: {missing_cols}")
        columns_to_save = [col for col in columns_to_save if col in df.columns]

    os.makedirs(os.path.dirname(output_csv_path) or '.', exist_ok=True)

    logger.info(f"Saving cleaned data to {output_csv_path}...")
    df[columns_to_save].to_csv(output_csv_path, index=False, encoding="utf-8-sig")
    logger.info(f"Successfully saved {len(df)} records to {output_csv_path}")
    
    return df

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Transform raw JSON data to cleaned CSV format",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="Path to raw JSON file (default: from config.yaml)"
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Path to save cleaned CSV file (default: from config.yaml)"
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
        global config
        config = load_config(args.config)
    
    transform_data(
        raw_json_path=args.input,
        output_csv_path=args.output
    )