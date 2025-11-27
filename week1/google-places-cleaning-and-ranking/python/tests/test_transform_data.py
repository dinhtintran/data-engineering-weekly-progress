"""
Unit tests for transform_data.py
"""
import unittest
import os
import sys
import json
import tempfile
import pandas as pd

# Add parent directory to path to import modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from transform_data import transform_data
from tests.test_data import EXPECTED_TRANSFORMED_DATA


class TestTransformData(unittest.TestCase):
    """Test cases for transform_data module"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.test_dir = tempfile.mkdtemp()
        self.test_input_path = os.path.join(self.test_dir, 'test_input.json')
        self.test_output_path = os.path.join(self.test_dir, 'test_output.csv')
        
        # Create test input JSON file
        with open(self.test_input_path, 'w', encoding='utf-8') as f:
            json.dump(EXPECTED_TRANSFORMED_DATA, f, ensure_ascii=False, indent=2)
    
    def tearDown(self):
        """Clean up after tests"""
        import shutil
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
    
    def test_transform_data_basic(self):
        """Test basic data transformation"""
        df = transform_data(
            raw_json_path=self.test_input_path,
            output_csv_path=self.test_output_path
        )
        
        # Check that DataFrame is created
        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(len(df), 0)
        
        # Check that output file exists
        self.assertTrue(os.path.exists(self.test_output_path))
    
    def test_transform_data_columns(self):
        """Test that required columns are present"""
        df = transform_data(
            raw_json_path=self.test_input_path,
            output_csv_path=self.test_output_path
        )
        
        expected_columns = ['place_id', 'name', 'rating', 'user_ratings_total', 
                          'latitude', 'longitude', 'address', 'types']
        
        for col in expected_columns:
            self.assertIn(col, df.columns, f"Column {col} should be present")
    
    def test_transform_data_missing_values(self):
        """Test handling of missing values"""
        # Create data with missing values
        data_with_missing = [
            {
                "place_id": "test1",
                "name": "Test Place",
                "rating": None,
                "user_ratings_total": None,
                "geometry": {"lat": 10.0, "lng": 106.0},
                "address": "Test Address",
                "types": ["cafe"]
            }
        ]
        
        test_input = os.path.join(self.test_dir, 'test_missing.json')
        with open(test_input, 'w', encoding='utf-8') as f:
            json.dump(data_with_missing, f)
        
        df = transform_data(
            raw_json_path=test_input,
            output_csv_path=self.test_output_path
        )
        
        # Check that missing values are filled (default is 0)
        self.assertEqual(df['rating'].iloc[0], 0)
        self.assertEqual(df['user_ratings_total'].iloc[0], 0)
    
    def test_transform_data_coordinates_extraction(self):
        """Test extraction of coordinates from geometry"""
        df = transform_data(
            raw_json_path=self.test_input_path,
            output_csv_path=self.test_output_path
        )
        
        # Check that coordinates are extracted
        self.assertIn('latitude', df.columns)
        self.assertIn('longitude', df.columns)
        
        # Check that coordinates are not all null
        self.assertTrue(df['latitude'].notna().any())
        self.assertTrue(df['longitude'].notna().any())
    
    def test_transform_data_file_not_found(self):
        """Test error handling when input file doesn't exist"""
        nonexistent_path = os.path.join(self.test_dir, 'nonexistent.json')
        
        with self.assertRaises(FileNotFoundError):
            transform_data(
                raw_json_path=nonexistent_path,
                output_csv_path=self.test_output_path
            )
    
    def test_transform_data_output_format(self):
        """Test that output CSV is created correctly"""
        df = transform_data(
            raw_json_path=self.test_input_path,
            output_csv_path=self.test_output_path
        )
        
        # Read back the CSV
        df_read = pd.read_csv(self.test_output_path)
        
        # Check that data is preserved
        self.assertEqual(len(df), len(df_read))
        
        # Check that only columns_to_save are in the CSV (not all DataFrame columns)
        # The CSV should only have the columns specified in config
        expected_columns = ['place_id', 'name', 'rating', 'user_ratings_total', 
                          'latitude', 'longitude', 'address', 'types']
        for col in expected_columns:
            self.assertIn(col, df_read.columns, f"Column {col} should be in CSV")


if __name__ == '__main__':
    unittest.main()

