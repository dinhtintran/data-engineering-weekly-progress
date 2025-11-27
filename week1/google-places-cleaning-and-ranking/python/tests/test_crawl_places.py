"""
Unit tests for crawl_places.py
"""
import unittest
import os
import sys
import json
import tempfile
from unittest.mock import Mock, patch, MagicMock

# Add parent directory to path to import modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import crawl_places at module level
import crawl_places
from tests.test_data import SAMPLE_APIFY_RESPONSE


class TestCrawlPlaces(unittest.TestCase):
    """Test cases for crawl_places module"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.test_dir = tempfile.mkdtemp()
        self.test_output_path = os.path.join(self.test_dir, 'test_output.json')
        self.test_query = "coffee shop, Ho Chi Minh City"
    
    def tearDown(self):
        """Clean up after tests"""
        import shutil
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
    
    @patch('crawl_places.ApifyClient')
    def test_crawl_raw_success(self, mock_client_class):
        """Test successful crawling of places"""
        # Mock Apify client
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        # Mock actor call - create a proper mock chain
        mock_actor = MagicMock()
        mock_run_result = {
            'id': 'test_run_id',
            'defaultDatasetId': 'test_dataset_id'
        }
        mock_actor.call.return_value = mock_run_result
        mock_client.actor.return_value = mock_actor
        
        # Mock dataset items
        mock_dataset = MagicMock()
        mock_dataset_items = MagicMock()
        mock_dataset_items.items = SAMPLE_APIFY_RESPONSE
        mock_dataset.list_items.return_value = mock_dataset_items
        mock_client.dataset.return_value = mock_dataset
        
        # Mock APIFY_TOKEN check
        with patch('crawl_places.APIFY_TOKEN', 'test_token'):
            # Run the function
            result = crawl_places.crawl_raw(
                query=self.test_query,
                save_path=self.test_output_path,
                max_crawled_places=25,
                max_reviews=5
            )
            
            # Check that result is returned
            self.assertIsInstance(result, list)
            self.assertEqual(len(result), len(SAMPLE_APIFY_RESPONSE))
            
            # Check that output file is created
            self.assertTrue(os.path.exists(self.test_output_path))
            
            # Check that data is saved correctly
            with open(self.test_output_path, 'r', encoding='utf-8') as f:
                saved_data = json.load(f)
            
            self.assertEqual(len(saved_data), len(SAMPLE_APIFY_RESPONSE))
            self.assertEqual(saved_data[0]['place_id'], SAMPLE_APIFY_RESPONSE[0]['placeId'])
    
    def test_crawl_raw_no_token(self):
        """Test error when APIFY_TOKEN is not set"""
        # Mock APIFY_TOKEN as None
        with patch('crawl_places.APIFY_TOKEN', None):
            with self.assertRaises(ValueError) as context:
                crawl_places.crawl_raw(
                    query=self.test_query,
                    save_path=self.test_output_path
                )
            
            self.assertIn('APIFY_TOKEN', str(context.exception))
    
    @patch('crawl_places.ApifyClient')
    def test_crawl_raw_data_transformation(self, mock_client_class):
        """Test that Apify data is transformed correctly"""
        # Mock Apify client
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        # Mock actor call
        mock_actor = MagicMock()
        mock_run_result = {
            'id': 'test_run_id',
            'defaultDatasetId': 'test_dataset_id'
        }
        mock_actor.call.return_value = mock_run_result
        mock_client.actor.return_value = mock_actor
        
        # Mock dataset items
        mock_dataset = MagicMock()
        mock_dataset_items = MagicMock()
        mock_dataset_items.items = SAMPLE_APIFY_RESPONSE
        mock_dataset.list_items.return_value = mock_dataset_items
        mock_client.dataset.return_value = mock_dataset
        
        # Mock APIFY_TOKEN
        with patch('crawl_places.APIFY_TOKEN', 'test_token'):
            # Run the function
            result = crawl_places.crawl_raw(
                query=self.test_query,
                save_path=self.test_output_path
            )
            
            # Check data transformation
            self.assertEqual(result[0]['place_id'], SAMPLE_APIFY_RESPONSE[0]['placeId'])
            self.assertEqual(result[0]['name'], SAMPLE_APIFY_RESPONSE[0]['title'])
            self.assertEqual(result[0]['rating'], SAMPLE_APIFY_RESPONSE[0]['totalScore'])
            self.assertEqual(result[0]['user_ratings_total'], SAMPLE_APIFY_RESPONSE[0]['reviewsCount'])
    
    @patch('crawl_places.ApifyClient')
    def test_crawl_raw_reviews_transformation(self, mock_client_class):
        """Test that reviews are transformed correctly"""
        # Mock Apify client
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        # Mock actor call
        mock_actor = MagicMock()
        mock_run_result = {
            'id': 'test_run_id',
            'defaultDatasetId': 'test_dataset_id'
        }
        mock_actor.call.return_value = mock_run_result
        mock_client.actor.return_value = mock_actor
        
        # Mock dataset items
        mock_dataset = MagicMock()
        mock_dataset_items = MagicMock()
        mock_dataset_items.items = SAMPLE_APIFY_RESPONSE
        mock_dataset.list_items.return_value = mock_dataset_items
        mock_client.dataset.return_value = mock_dataset
        
        # Mock APIFY_TOKEN
        with patch('crawl_places.APIFY_TOKEN', 'test_token'):
            # Run the function
            result = crawl_places.crawl_raw(
                query=self.test_query,
                save_path=self.test_output_path
            )
            
            # Check reviews transformation
            self.assertEqual(len(result[0]['reviews']), len(SAMPLE_APIFY_RESPONSE[0]['reviews']))
            self.assertEqual(result[0]['reviews'][0]['author_name'], 
                             SAMPLE_APIFY_RESPONSE[0]['reviews'][0]['name'])
            self.assertEqual(result[0]['reviews'][0]['rating'], 
                             SAMPLE_APIFY_RESPONSE[0]['reviews'][0]['stars'])


if __name__ == '__main__':
    unittest.main()

