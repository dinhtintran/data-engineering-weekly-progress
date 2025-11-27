# 🗺️ Google Places Cleaning and Ranking

![status](https://img.shields.io/badge/Status-Completed-brightgreen)
![python](https://img.shields.io/badge/Python-3.12-blue)
![sql](https://img.shields.io/badge/SQL-SQLite-yellow)

---

## 📋 Project Overview

This is a complete ETL pipeline project that:
1. **Extract**: Crawls Google Places data using Apify API
2. **Transform**: Cleans and processes raw JSON data into structured CSV format
3. **Load**: Stores data in SQLite database and generates ranked reports using SQL window functions

---

## 🏗️ Project Structure

```
week1/
├── google-places-cleaning-and-ranking/
│   ├── data/
│   │   ├── raw/          # Raw JSON data from Apify
│   │   └── clean/         # Cleaned CSV data
│   ├── output/            # Final outputs (database, ranked CSV)
│   ├── python/
│   │   ├── crawl_places.py      # Extract: Crawl Google Places data
│   │   ├── transform_data.py    # Transform: Clean and process data
│   │   └── config_loader.py     # Configuration loader utility
│   ├── sql/
│   │   └── run.sql              # Load: SQLite operations and ranking
│   └── config.yaml              # Configuration file for paths and settings
└── README.md
```

---

## 🚀 Getting Started

### Prerequisites

- Python 3.12+
- SQLite3
- Apify account and API token
- PyYAML (for configuration file support)

### Installation

1. **Create and activate virtual environment:**

   On macOS/Linux:
   ```bash
   # Navigate to project root
   cd /Users/tin/Desktop/data-engineer-weekly-progress/data-engineering-weekly-progress
   
   # Create virtual environment
   python3 -m venv venv
   
   # Activate virtual environment
   source venv/bin/activate
   ```

   On Windows:
   ```bash
   # Navigate to project root
   cd C:\path\to\data-engineering-weekly-progress
   
   # Create virtual environment
   python -m venv venv
   
   # Activate virtual environment
   venv\Scripts\activate
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```
   
   This will install all required packages including `pyyaml` for configuration file support.

3. **Set up environment variables:**
   
   Copy `.env.example` to `.env` in the project root (same level as `requirements.txt`):
   ```bash
   # On macOS/Linux
   cp .env.example .env
   
   # On Windows
   copy .env.example .env
   ```
   
   Then edit the `.env` file and replace `your_apify_token_here` with your actual Apify token:
   ```env
   APIFY_TOKEN=your_actual_apify_token
   ```
   
   > ⚠️ **Note**: Never commit `.env` file to git! It should be in `.gitignore`. Only `.env.example` should be committed.

4. **Deactivate virtual environment (when done):**
   ```bash
   deactivate
   ```

### How to Get Apify API Key

1. Go to [Apify Console](https://console.apify.com/)
2. Sign up or log in
3. Navigate to **Settings** → **Integrations** → **API tokens**
4. Copy your API token
5. Add it to your `.env` file as `APIFY_TOKEN`

---

## ⚡ Quick Start

```bash
# 1. Create and activate venv
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Set up .env file from .env.example
cp .env.example .env
# Then edit .env and add your actual APIFY_TOKEN

# 4. Run the ETL pipeline
cd week1/google-places-cleaning-and-ranking/python

# Step 1: Extract (with unique output filename)
python crawl_places.py "billiard, Ho Chi Minh City" --max-crawled-places 25 --max-reviews 5 --output ../data/raw/billiard_places.json

# Step 2: Transform (update paths in transform_data.py first!)
# Edit transform_data.py: RAW_JSON_PATH = "../data/raw/billiard_places.json"
# Edit transform_data.py: output_csv_path="../data/clean/billiard_clean_place.csv"
python transform_data.py

# Step 3: Load & Rank (update paths in run.sql first!)
# Edit run.sql: .import path and .output path
cd ../sql
sqlite3 ../output/billiard_places.db < run.sql
```

> 💡 **Tip**: For each new query, use a naming convention like `{query_type}_places.json` to keep your data organized!

---

## 📖 Usage

> ⚠️ **Important**: Make sure your virtual environment is activated before running the scripts!
> 
> 💡 **Note**: The project now uses `config.yaml` to manage file paths and settings. You can edit `config.yaml` to change default paths, or override them via CLI arguments.

### Step 1: Extract Data (Crawl)

```bash
# Navigate to python directory
cd week1/google-places-cleaning-and-ranking/python

# Make sure venv is activated (you should see (venv) in your terminal)

# Basic usage - prompt for query input (with defaults: max 25 places, max 5 reviews per place)
python crawl_places.py

# With query as argument
python crawl_places.py "billiard, Ho Chi Minh City"

# With custom parameters
python crawl_places.py "coffee shop, New York" --max-crawled-places 50 --max-reviews 10

# With custom output path (IMPORTANT: Use different file names for different queries!)
python crawl_places.py "spa, Ho Chi Minh City" --max-crawled-places 30 --output ../data/raw/spa_places.json
python crawl_places.py "billiard, Ho Chi Minh City" --output ../data/raw/billiard_places.json
python crawl_places.py "coffee shop, New York" --output ../data/raw/coffeeshop_places.json

# Using custom config file
python crawl_places.py "query" --config ../custom_config.yaml
```

**CLI Arguments:**
- `query` (optional): Search query for places (e.g., `'billiard, Ho Chi Minh City'`). If not provided, will prompt for input.
- `--max-crawled-places` (optional, default from config): Maximum number of places to crawl
- `--max-reviews` (optional, default from config): Maximum number of reviews per place
- `--output` (optional, default from config): Path to save raw JSON data
- `--config` (optional): Path to custom config.yaml file

> ⚠️ **Important**: CLI arguments override config.yaml values. Always specify a unique `--output` filename for each different query to avoid overwriting previous data!

The script will:
- Connect to Apify API
- Crawl Google Places data with specified parameters
- Save raw JSON to the specified output path

**Logs**: Check `crawl_places.log` for detailed execution logs

### Step 2: Transform Data (Clean)

```bash
# Still in python directory (or navigate there if needed)

# Using defaults from config.yaml
python transform_data.py

# With custom input/output paths
python transform_data.py --input ../data/raw/billiard_places.json --output ../data/clean/billiard_clean_place.csv

# Using custom config file
python transform_data.py --config ../custom_config.yaml
```

**CLI Arguments:**
- `--input` (optional, default from config): Path to raw JSON file
- `--output` (optional, default from config): Path to save cleaned CSV file
- `--config` (optional): Path to custom config.yaml file

> 💡 **Note**: You can now use CLI arguments instead of editing the script! The script reads default paths from `config.yaml`, but CLI arguments override them.

The script will:
- Load raw JSON data from the specified path (config or CLI)
- Handle missing values using config defaults
- Extract coordinates from geometry
- Save cleaned CSV to the specified output path

**Logs**: Check `transform_data.log` for detailed execution logs

### Step 3: Load & Rank (SQL)

**⚠️ Important**: Before running the SQL script, you need to update the file paths in `run.sql`:

1. **Edit `run.sql`** and change:
   - Line 4: `.import` path → your cleaned CSV file path
   - Line 43: `.output` path → your desired output CSV path
   - Database name (optional): Change `places.db` to a unique name for each dataset

   Example:
   ```sql
   -- For billiard data
   .import /path/to/week1/google-places-cleaning-and-ranking/data/clean/billiard_clean_place.csv places
   .output /path/to/week1/google-places-cleaning-and-ranking/output/billiard_ranked_places.csv
   
   -- For coffee shop data
   .import /path/to/week1/google-places-cleaning-and-ranking/data/clean/coffeeshop_clean_place.csv places
   .output /path/to/week1/google-places-cleaning-and-ranking/output/coffeeshop_ranked_places.csv
   ```

2. **Run the SQL script:**
   ```bash
   cd ../sql
   sqlite3 ../output/places.db < run.sql
   # Or use a unique database name:
   sqlite3 ../output/billiard_places.db < run.sql
   ```

The SQL script will:
- Create SQLite database
- Import cleaned CSV data from the specified path
- Clean and extract category types
- Generate ranked places by category using `RANK()` and `DENSE_RANK()` window functions
- Export ranked results to the specified output CSV path

---

## 🔍 Key Features

### Python Scripts

- **Logging**: Comprehensive logging to both file and console
- **Error Handling**: Proper exception handling and validation
- **Data Cleaning**: Missing value handling, coordinate extraction
- **Modular Design**: Functions can be imported and reused
- **CLI Arguments**: Flexible command-line interface for customization

### SQL Operations

- **Window Functions**: Uses `RANK()` and `DENSE_RANK()` to rank places within categories
- **Data Cleaning**: String manipulation to extract clean category names
- **Partitioning**: Ranks places by category (e.g., billiard halls, spas)
- **Ordering**: Sorts by rating (DESC) then review count (DESC)

---

## 📊 Output Files

| File | Description |
|------|-------------|
| `google-places-cleaning-and-ranking/data/raw/*.json` | Raw JSON data from Apify |
| `google-places-cleaning-and-ranking/data/clean/*.csv` | Cleaned CSV with structured data |
| `google-places-cleaning-and-ranking/output/places.db` | SQLite database with places data |
| `google-places-cleaning-and-ranking/output/ranked_places.csv` | Final ranked results by category |

---

## 🛠️ Technologies Used

- **Python**: pandas, apify-client, python-dotenv
- **SQL**: SQLite with window functions
- **APIs**: Apify Google Maps Scraper
- **Data Formats**: JSON, CSV, SQLite

---

## 📝 Notes

### Important File Path Management

⚠️ **When running with different queries, you MUST update file paths to avoid overwriting data:**

**Option 1: Using config.yaml (Recommended)**
1. Edit `config.yaml` to change default filenames:
   ```yaml
   paths:
     default_raw_json: "billiard_places.json"
     default_clean_csv: "billiard_clean_place.csv"
   sql:
     database_name: "billiard_places.db"
   ```

**Option 2: Using CLI Arguments (Override config)**
1. **`crawl_places.py`**: Use `--output` argument
   ```bash
   python crawl_places.py "query1" --output ../data/raw/query1_places.json
   python crawl_places.py "query2" --output ../data/raw/query2_places.json
   ```

2. **`transform_data.py`**: Use `--input` and `--output` arguments
   ```bash
   python transform_data.py --input ../data/raw/query1_places.json --output ../data/clean/query1_clean.csv
   ```

3. **`run.sql`**: Edit the SQL file to change:
   - `.import` path (line 4) → your cleaned CSV file path
   - `.output` path (line 43) → your ranked output CSV path
   - Database name (optional) → use unique names like `query1_places.db`

### Configuration Management

- **Centralized Configuration**: All paths and settings are managed in `config.yaml`
- **CLI Override**: CLI arguments can override config.yaml values
- **Flexible**: Easy to switch between different datasets by editing config or using CLI args

### Other Notes

- The project uses SQLite for simplicity (no PostgreSQL setup required)
- Window functions (`RANK()` and `DENSE_RANK()`) are used to rank places within each category
- Logging is implemented for both scripts to track execution and debug issues
- Raw data is preserved in `data/raw/` for reproducibility
- Configuration file (`config.yaml`) centralizes all settings for easier maintenance

---

## ✅ Learning Outcomes

This project demonstrates:
- ✅ ETL pipeline design (Extract → Transform → Load)
- ✅ Python data processing with pandas
- ✅ SQL window functions and ranking
- ✅ Logging and error handling
- ✅ API integration (Apify)
- ✅ File I/O (JSON, CSV, SQLite)
- ✅ CLI argument parsing with argparse

---

## 🔗 Related Resources

- [Apify Google Maps Scraper](https://apify.com/compass/crawler-google-places)
- [SQLite Window Functions](https://www.sqlite.org/windowfunctions.html)
- [Pandas Documentation](https://pandas.pydata.org/docs/)

---

## 📁 Project Files

- **Python Scripts**: 
  - [`google-places-cleaning-and-ranking/python/crawl_places.py`](./google-places-cleaning-and-ranking/python/crawl_places.py) - Extract data from Apify
  - [`google-places-cleaning-and-ranking/python/transform_data.py`](./google-places-cleaning-and-ranking/python/transform_data.py) - Transform and clean data
- **SQL Scripts**: [`google-places-cleaning-and-ranking/sql/run.sql`](./google-places-cleaning-and-ranking/sql/run.sql) - Load data and generate rankings
