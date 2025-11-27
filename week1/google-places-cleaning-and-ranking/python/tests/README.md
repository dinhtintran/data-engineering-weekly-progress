# 🧪 Test Suite Documentation

This directory contains comprehensive unit tests for the Google Places ETL pipeline.

---

## 📋 Overview

The test suite uses Python's built-in `unittest` framework to test all major components of the ETL pipeline:
- Configuration loading
- Data transformation
- API crawling (with mocked API calls)

All tests use **mock data** and **mocked API calls**, so no actual API credentials or network calls are required.

---

## 📁 Test Files

### `test_config_loader.py`
Tests for the configuration loader utility (`config_loader.py`).

| Test Case | Description | Status |
|-----------|-------------|--------|
| `test_get_default_config()` | Verifies default configuration structure | ✅ |
| `test_load_config_with_valid_file()` | Tests loading config from valid YAML file | ✅ |
| `test_load_config_with_invalid_file()` | Tests fallback to default config when file not found | ✅ |
| `test_load_config_with_none_path()` | Tests automatic config file discovery | ✅ |
| `test_get_path_valid_nested()` | Tests getting nested config values | ✅ |
| `test_get_path_invalid_key()` | Tests handling of invalid keys | ✅ |
| `test_get_path_with_default()` | Tests default value fallback | ✅ |
| `test_get_path_deeply_nested()` | Tests deeply nested path resolution | ✅ |
| `test_get_path_nonexistent_intermediate()` | Tests handling of nonexistent intermediate keys | ✅ |

**Coverage:**
- Config file loading (YAML parsing)
- Default configuration fallback
- Nested path resolution
- Error handling

---

### `test_transform_data.py`
Tests for the data transformation module (`transform_data.py`).

| Test Case | Description | Status |
|-----------|-------------|--------|
| `test_transform_data_basic()` | Tests basic data transformation workflow | ✅ |
| `test_transform_data_columns()` | Verifies required columns are present in output | ✅ |
| `test_transform_data_missing_values()` | Tests handling of missing/null values | ✅ |
| `test_transform_data_coordinates_extraction()` | Tests extraction of lat/lng from geometry | ✅ |
| `test_transform_data_file_not_found()` | Tests error handling for missing input files | ✅ |
| `test_transform_data_output_format()` | Verifies CSV output format and data preservation | ✅ |

**Coverage:**
- JSON to DataFrame conversion
- Missing value handling (fillna with defaults)
- Coordinate extraction from nested geometry objects
- Column selection and CSV export
- File I/O operations
- Error handling

**Test Data:**
- Uses `EXPECTED_TRANSFORMED_DATA` from `test_data.py`
- Creates temporary JSON files for testing
- Validates CSV output format

---

### `test_crawl_places.py`
Tests for the web crawling module (`crawl_places.py`).

| Test Case | Description | Status |
|-----------|-------------|--------|
| `test_crawl_raw_success()` | Tests successful crawling with mocked Apify API | ✅ |
| `test_crawl_raw_no_token()` | Tests error handling when APIFY_TOKEN is missing | ✅ |
| `test_crawl_raw_data_transformation()` | Tests transformation of Apify response to standard format | ✅ |
| `test_crawl_raw_reviews_transformation()` | Tests transformation of review data | ✅ |

**Coverage:**
- Apify API client mocking
- Data transformation (Apify format → standard format)
- Review data transformation
- File output (JSON saving)
- Error handling (missing API token)

**Mocking Strategy:**
- `ApifyClient` is fully mocked - no real API calls
- Mock actor responses with test data
- Mock dataset retrieval
- Mock APIFY_TOKEN for authentication tests

**Test Data:**
- Uses `SAMPLE_APIFY_RESPONSE` from `test_data.py`
- Simulates Apify API response structure
- Tests data transformation logic

---

### `test_data.py`
Mock data and fixtures for all tests.

| Variable | Description | Used In |
|----------|-------------|---------|
| `SAMPLE_APIFY_RESPONSE` | Sample Apify API response data | `test_crawl_places.py` |
| `EXPECTED_TRANSFORMED_DATA` | Expected output after transformation | `test_transform_data.py` |
| `SAMPLE_CONFIG` | Sample configuration for testing | `test_config_loader.py` |

**Purpose:**
- Centralized test data
- Consistent test fixtures
- Easy to update test scenarios

---

## 🚀 Running Tests

| Command | Description |
|---------|-------------|
| `python run_tests.py` | Run all tests and generate report |
| `python -m unittest tests.test_config_loader` | Run config loader tests only |
| `python -m unittest tests.test_transform_data` | Run transform data tests only |
| `python -m unittest tests.test_crawl_places` | Run crawl places tests only |
| `python -m unittest tests.test_config_loader.TestConfigLoader.test_get_default_config` | Run specific test case |
| `python -m unittest discover tests -v` | Run all tests with verbose output |
| `coverage run -m unittest discover tests` | Run tests with coverage (requires coverage.py) |
| `coverage report` | Show coverage report |
| `coverage html` | Generate HTML coverage report |

**Example:**
```bash
cd week1/google-places-cleaning-and-ranking/python
python run_tests.py
```

---

## 📊 Test Results

After running tests, a detailed report is automatically generated in:
```
python/test_reports/test_report_YYYYMMDD_HHMMSS.txt
```

**Report Contents:**
- Test summary (total tests, successes, failures, errors)
- Success rate percentage
- Detailed failure messages
- Error stack traces
- Timestamp

---

## ✅ Test Coverage

| Category | Feature | Status |
|----------|---------|--------|
| **Configuration Management** | Config file loading (YAML) | ✅ |
| | Default config fallback | ✅ |
| | Nested path resolution | ✅ |
| | Error handling | ✅ |
| **Data Transformation** | JSON to DataFrame conversion | ✅ |
| | Missing value handling | ✅ |
| | Coordinate extraction | ✅ |
| | Column selection | ✅ |
| | CSV export | ✅ |
| | File I/O error handling | ✅ |
| **API Integration (Mocked)** | Apify client initialization | ✅ |
| | API call mocking | ✅ |
| | Response transformation | ✅ |
| | Review data transformation | ✅ |
| | Error handling (missing token) | ✅ |

---

## 🔧 Test Utilities

### Temporary Files
All tests use Python's `tempfile` module to create temporary directories and files:
- Automatically cleaned up after tests
- No interference with actual project data
- Isolated test environment

### Mocking
- **ApifyClient**: Fully mocked to prevent real API calls
- **APIFY_TOKEN**: Patched for authentication tests
- **File I/O**: Uses temporary files

---

## 📝 Writing New Tests

### Test Structure
```python
import unittest
from unittest.mock import patch, MagicMock

class TestYourModule(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures"""
        pass
    
    def tearDown(self):
        """Clean up after tests"""
        pass
    
    def test_your_function(self):
        """Test description"""
        # Arrange
        # Act
        # Assert
        pass
```

### Best Practices
1. **Isolation**: Each test should be independent
2. **Naming**: Use descriptive test names (`test_what_when_expected`)
3. **Mocking**: Mock external dependencies (APIs, file system)
4. **Cleanup**: Always clean up temporary files/resources
5. **Assertions**: Use specific assertions (`assertEqual`, `assertIn`, etc.)

---

## 🐛 Troubleshooting

| Issue | Solution |
|-------|----------|
| **Import Errors** | Ensure you're running tests from the `python/` directory<br>Check that all dependencies are installed: `pip install -r requirements.txt` |
| **Mock Not Working** | Verify patch path matches the import path<br>Check that you're patching before the function is called<br>Use `patch.object` for instance methods |
| **File Not Found** | Tests use temporary files - check `setUp()` and `tearDown()` methods<br>Ensure file paths are relative to test execution directory |
| **API Calls Still Happening** | Verify `ApifyClient` is properly mocked<br>Check that mock is applied before client instantiation |

---

## 📈 Test Statistics

| Metric | Value |
|--------|-------|
| **Total Test Cases** | 19 |
| **Config Loader Tests** | 9 |
| **Transform Data Tests** | 6 |
| **Crawl Places Tests** | 4 |
| **Test Execution Time** | ~1-2 seconds (with mocks) |
| **Success Rate** | 100% (all tests passing) |

| Coverage Area | Status |
|---------------|--------|
| Configuration management | ✅ |
| Data transformation pipeline | ✅ |
| API integration (mocked) | ✅ |
| Error handling | ✅ |
| File I/O operations | ✅ |

---

## 🔗 Related Files

- `run_tests.py` - Test runner script
- `test_data.py` - Mock data and fixtures
- `../config_loader.py` - Configuration module
- `../transform_data.py` - Transformation module
- `../crawl_places.py` - Crawling module

---

## 📚 Additional Resources

- [Python unittest Documentation](https://docs.python.org/3/library/unittest.html)
- [unittest.mock Documentation](https://docs.python.org/3/library/unittest.mock.html)
- [Testing Best Practices](https://docs.python-guide.org/writing/tests/)

---

**Last Updated:** 2025-11-27

