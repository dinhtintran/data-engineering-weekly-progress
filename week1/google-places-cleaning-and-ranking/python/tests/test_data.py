"""
Mock data for testing
"""
import os

# Sample Apify API response data
SAMPLE_APIFY_RESPONSE = [
    {
        "placeId": "ChIJN1t_tDeuEmsRUsoyG83frY4",
        "title": "Test Coffee Shop 1",
        "totalScore": 4.5,
        "reviewsCount": 120,
        "placeLocation": {"lat": 10.762622, "lng": 106.660172},
        "address": "123 Test Street, Ho Chi Minh City",
        "categories": ["cafe", "restaurant"],
        "reviews": [
            {
                "name": "John Doe",
                "stars": 5,
                "text": "Great coffee!",
                "publishedAtDate": "2024-01-15T10:00:00Z"
            },
            {
                "name": "Jane Smith",
                "stars": 4,
                "text": "Nice atmosphere",
                "publishedAtDate": "2024-01-10T14:30:00Z"
            }
        ]
    },
    {
        "placeId": "ChIJN1t_tDeuEmsRUsoyG83frY5",
        "title": "Test Coffee Shop 2",
        "totalScore": 4.2,
        "reviewsCount": 85,
        "placeLocation": {"lat": 10.775622, "lng": 106.670172},
        "address": "456 Test Avenue, Ho Chi Minh City",
        "categories": ["cafe"],
        "reviews": [
            {
                "name": "Bob Wilson",
                "stars": 5,
                "text": "Excellent service",
                "publishedAtDate": "2024-01-12T09:00:00Z"
            }
        ]
    },
    {
        "placeId": "ChIJN1t_tDeuEmsRUsoyG83frY6",
        "title": "Test Coffee Shop 3",
        "totalScore": None,  # Missing rating
        "reviewsCount": None,  # Missing review count
        "location": {"lat": 10.785622, "lng": 106.680172},  # Different key
        "address": "789 Test Road, Ho Chi Minh City",
        "categories": ["cafe", "bakery"],
        "reviews": []
    }
]

# Expected transformed data
EXPECTED_TRANSFORMED_DATA = [
    {
        "place_id": "ChIJN1t_tDeuEmsRUsoyG83frY4",
        "name": "Test Coffee Shop 1",
        "rating": 4.5,
        "user_ratings_total": 120,
        "geometry": {"lat": 10.762622, "lng": 106.660172},
        "address": "123 Test Street, Ho Chi Minh City",
        "types": ["cafe", "restaurant"],
        "reviews": [
            {
                "author_name": "John Doe",
                "rating": 5,
                "text": "Great coffee!",
                "time": "2024-01-15T10:00:00Z"
            },
            {
                "author_name": "Jane Smith",
                "rating": 4,
                "text": "Nice atmosphere",
                "time": "2024-01-10T14:30:00Z"
            }
        ]
    },
    {
        "place_id": "ChIJN1t_tDeuEmsRUsoyG83frY5",
        "name": "Test Coffee Shop 2",
        "rating": 4.2,
        "user_ratings_total": 85,
        "geometry": {"lat": 10.775622, "lng": 106.670172},
        "address": "456 Test Avenue, Ho Chi Minh City",
        "types": ["cafe"],
        "reviews": [
            {
                "author_name": "Bob Wilson",
                "rating": 5,
                "text": "Excellent service",
                "time": "2024-01-12T09:00:00Z"
            }
        ]
    },
    {
        "place_id": "ChIJN1t_tDeuEmsRUsoyG83frY6",
        "name": "Test Coffee Shop 3",
        "rating": None,
        "user_ratings_total": None,
        "geometry": {"lat": 10.785622, "lng": 106.680172},
        "address": "789 Test Road, Ho Chi Minh City",
        "types": ["cafe", "bakery"],
        "reviews": []
    }
]

# Sample config data
SAMPLE_CONFIG = {
    'paths': {
        'raw_data_dir': '../data/raw',
        'clean_data_dir': '../data/clean',
        'output_dir': '../output',
        'default_raw_json': 'test_raw.json',
        'default_clean_csv': 'test_clean.csv'
    },
    'apify': {
        'actor_id': 'compass/crawler-google-places',
        'default_max_places': 25,
        'default_max_reviews': 5
    },
    'processing': {
        'columns_to_save': ['place_id', 'name', 'rating'],
        'default_rating': 0,
        'default_user_ratings_total': 0
    }
}

