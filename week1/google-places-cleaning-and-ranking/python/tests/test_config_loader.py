"""
Unit tests for config_loader.py
"""
import unittest
import os
import tempfile
import yaml
import sys

# Add parent directory to path to import modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config_loader import load_config, get_path, get_default_config
from tests.test_data import SAMPLE_CONFIG


class TestConfigLoader(unittest.TestCase):
    """Test cases for config_loader module"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.test_dir = tempfile.mkdtemp()
        self.test_config_path = os.path.join(self.test_dir, 'test_config.yaml')
    
    def tearDown(self):
        """Clean up after tests"""
        import shutil
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
    
    def test_get_default_config(self):
        """Test that default config is returned correctly"""
        config = get_default_config()
        
        self.assertIsInstance(config, dict)
        self.assertIn('paths', config)
        self.assertIn('apify', config)
        self.assertIn('processing', config)
        self.assertIn('sql', config)
        self.assertIn('logging', config)
    
    def test_load_config_with_valid_file(self):
        """Test loading config from valid YAML file"""
        # Create a test config file
        with open(self.test_config_path, 'w') as f:
            yaml.dump(SAMPLE_CONFIG, f)
        
        config = load_config(self.test_config_path)
        
        self.assertEqual(config['paths']['default_raw_json'], 'test_raw.json')
        self.assertEqual(config['apify']['default_max_places'], 25)
    
    def test_load_config_with_invalid_file(self):
        """Test loading config with invalid file path"""
        invalid_path = os.path.join(self.test_dir, 'nonexistent.yaml')
        config = load_config(invalid_path)
        
        # Should return default config
        self.assertIsInstance(config, dict)
        self.assertIn('paths', config)
    
    def test_load_config_with_none_path(self):
        """Test loading config with None path (should search for config.yaml)"""
        # This will use default config if config.yaml doesn't exist
        config = load_config(None)
        
        self.assertIsInstance(config, dict)
        self.assertIn('paths', config)
    
    def test_get_path_valid_nested(self):
        """Test getting nested path from config"""
        config = SAMPLE_CONFIG
        value = get_path(config, 'paths', 'default_raw_json')
        
        self.assertEqual(value, 'test_raw.json')
    
    def test_get_path_invalid_key(self):
        """Test getting path with invalid key"""
        config = SAMPLE_CONFIG
        value = get_path(config, 'paths', 'nonexistent_key')
        
        self.assertIsNone(value)
    
    def test_get_path_with_default(self):
        """Test getting path with default value"""
        config = SAMPLE_CONFIG
        value = get_path(config, 'paths', 'nonexistent_key', default='default_value')
        
        self.assertEqual(value, 'default_value')
    
    def test_get_path_deeply_nested(self):
        """Test getting deeply nested path"""
        config = {
            'level1': {
                'level2': {
                    'level3': 'value'
                }
            }
        }
        value = get_path(config, 'level1', 'level2', 'level3')
        
        self.assertEqual(value, 'value')
    
    def test_get_path_nonexistent_intermediate(self):
        """Test getting path with nonexistent intermediate key"""
        config = SAMPLE_CONFIG
        value = get_path(config, 'nonexistent', 'key', default='default')
        
        self.assertEqual(value, 'default')


if __name__ == '__main__':
    unittest.main()

