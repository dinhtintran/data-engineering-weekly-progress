"""
Configuration loader utility for ETL pipeline
"""
import yaml
import os
import logging

logger = logging.getLogger(__name__)

def load_config(config_path=None):
    """
    Load configuration from YAML file
    
    Args:
        config_path (str): Path to config.yaml file. If None, searches in parent directory.
    
    Returns:
        dict: Configuration dictionary
    """
    if config_path is None:
        # Try to find config.yaml in parent directory
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(script_dir, "..", "config.yaml")
    
    if not os.path.exists(config_path):
        logger.warning(f"Config file not found at {config_path}, using defaults")
        return get_default_config()
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        logger.info(f"Loaded configuration from {config_path}")
        return config
    except Exception as e:
        logger.error(f"Error loading config file: {str(e)}")
        logger.info("Using default configuration")
        return get_default_config()

def get_default_config():
    """Return default configuration if config file is not found"""
    return {
        'paths': {
            'raw_data_dir': '../data/raw',
            'clean_data_dir': '../data/clean',
            'output_dir': '../output',
            'default_raw_json': 'raw_places.json',
            'default_clean_csv': 'clean_places.csv',
            'default_database': 'places.db',
            'default_ranked_csv': 'ranked_places.csv'
        },
        'apify': {
            'actor_id': 'compass/crawler-google-places',
            'default_max_places': 25,
            'default_max_reviews': 5,
            'scrape_place_detail_page': False,
            'reviews_sort': 'newest'
        },
        'processing': {
            'columns_to_save': [
                'place_id', 'name', 'rating', 'user_ratings_total',
                'latitude', 'longitude', 'address', 'types'
            ],
            'default_rating': 0,
            'default_user_ratings_total': 0
        },
        'sql': {
            'database_name': 'places.db',
            'places_table': 'places',
            'ranking_table': 'place_ranking',
            'use_dense_rank': True,
            'ranking_limit': 20
        },
        'logging': {
            'log_dir': '.',
            'crawl_log_file': 'crawl_places.log',
            'transform_log_file': 'transform_data.log',
            'log_level': 'INFO'
        }
    }

def get_path(config, *keys, default=None):
    """
    Safely get nested config value
    
    Args:
        config (dict): Configuration dictionary
        *keys: Keys to traverse
        default: Default value if key not found
    
    Returns:
        Value from config or default
    """
    value = config
    for key in keys:
        if isinstance(value, dict):
            value = value.get(key)
            if value is None:
                return default
        else:
            return default
    return value if value is not None else default

