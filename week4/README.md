# Week 4: Spark Mini Project - Place Reviews Analysis

## Introduction to Apache Spark

### What is Apache Spark?

Apache Spark is an open-source, distributed computing framework designed for fast, large-scale data processing. It's widely used in data engineering for ETL (Extract, Transform, Load) pipelines, data analytics, and machine learning.

**Key Characteristics:**
- **In-Memory Processing**: Spark keeps data in RAM during computation, making it much faster than disk-based systems like Hadoop MapReduce
- **Lazy Evaluation**: Spark doesn't execute operations immediately; instead, it builds a DAG (Directed Acyclic Graph) and optimizes it before execution
- **Distributed**: Handles large datasets across clusters of computers
- **Multiple Interfaces**: Supports Python (PySpark), Scala, Java, SQL, and R

### Core Concepts

#### 1. **RDD vs DataFrame vs Dataset**
- **RDD** (Resilient Distributed Dataset): Low-level API, less optimized
- **DataFrame**: Structured data like a SQL table (recommended for most use cases)
- **Dataset**: Type-safe, compiled for Scala/Java

#### 2. **Transformations vs Actions**

**Transformations** (Lazy Operations):
- Create new RDD/DataFrame from existing ones
- NOT executed immediately
- Examples: `filter()`, `map()`, `join()`, `groupBy()`
- Return another RDD/DataFrame

**Actions** (Eager Operations):
- Trigger actual computation
- Return results to driver or write to storage
- Examples: `show()`, `count()`, `collect()`, `write()`

**Example:**
```python
# Transformation - NOT executed yet
filtered = df.filter(col("age") > 25)

# Action - NOW Spark actually runs the job!
filtered.show()  # This triggers execution
```

#### 3. **Lazy Execution / Lazy Evaluation**

Spark optimizes queries before execution:
```
1. User writes: df.filter(...).join(...).groupBy(...).show()
2. Spark builds a DAG (logical plan)
3. Spark applies optimizations (Catalyst optimizer)
4. Spark creates execution plan (physical plan)
5. When .show() is called → execution happens!
```

You can inspect the execution plan with `.explain()`:
```python
df.explain(True)  # Shows full execution plan
```

#### 4. **Key Functions**

| Function | Type | Description |
|----------|------|-------------|
| `filter()` | Transformation | Keep rows matching condition |
| `select()` | Transformation | Choose specific columns |
| `join()` | Transformation | Combine two DataFrames |
| `groupBy()` | Transformation | Group by column(s) |
| `count()` | Action | Count rows |
| `show()` | Action | Display sample rows |
| `write()` | Action | Write to storage |
| `collect()` | Action | Bring all data to driver |

---

## Project Overview: Place Reviews Analysis

### Project Goal
This project analyzes customer reviews for places (restaurants, shops, etc.) to identify which regions have the most five-star reviews. It processes review data by filtering meaningful reviews, enriching them with geographic information, and aggregating results by region.

### Business Logic

**Input Data:**
1. **reviews.csv** - Customer reviews with place identifiers and review text
   - Columns: `place identifier`, `review text`, `star rating`
   
2. **dim_place.csv** - Place dimension table with location info
   - Columns: `place identifier`, `region`

**Processing Steps:**

```
Reviews Data + Places Data
        ↓
[1] Filter reviews by minimum word count (default: 10 words)
        ↓ (removes short/spam reviews)
[2] Join with places to get region information
        ↓ (enrich reviews with geographic data)
[3] Filter for 5-star reviews only
        ↓ (focus on excellent reviews)
[4] Aggregate count by region
        ↓
Output: Count of 5-star reviews per region
```

**Output:**
Parquet files partitioned by region, showing the count of 5-star reviews in each region.

### Project Structure

```
spark-reviews-week4/
├── data/
│   ├── reviews.csv              # Review records
│   └── dim_place.csv            # Place dimension with regions
├── output/
│   └── five_star_by_region/     # Output partitioned by region
├── src/
│   ├── main.py                  # Main entry point and orchestration
│   ├── transform.py             # Data transformation logic
│   ├── utils.py                 # Helper functions (logging, file reading, Spark session)
│   └── __pycache__/
├── tests/
│   ├── test_transform.py        # Unit tests using pytest
│   └── README.md
└── requirements.txt             # Python dependencies
```

### Code Components

#### 1. **main.py** - Orchestration
- Parses command-line arguments
- Creates Spark session
- Reads input data (CSV/Parquet/JSON support)
- Calls transformation functions in sequence
- Displays results and writes output
- Includes logging for tracking execution

**Key Arguments:**
```bash
--reviews_path       Path to reviews file (default: data/reviews.csv)
--places_path        Path to places file (default: data/dim_place.csv)
--output_path        Output directory (default: output/five_star_by_region)
--min_words          Minimum words in review (default: 10)
```

#### 2. **transform.py** - Transformation Logic
Core functions that implement the business logic:

- **`validate_schema()`**: Ensures DataFrames have required columns before processing
- **`filter_min_words()`**: Filters reviews by minimum word count, logs null values
- **`join_place_region()`**: Joins review and place data using inner join
- **`agg_five_star_by_region()`**: Counts 5-star reviews grouped by region

Each function includes:
- Schema validation (raises error if columns missing)
- Logging for debugging
- Null value checking

#### 3. **utils.py** - Utilities
Helper functions for common tasks:

- **`setup_logger()`**: Configures Python logging with formatted output
- **`read_data()`**: Auto-detects file format (.csv, .parquet, .json) and reads accordingly
- **`create_spark()`**: Initializes Spark session

#### 4. **tests/test_transform.py** - Unit Tests
Comprehensive pytest-based tests covering:
- Schema validation success and failure cases
- Filtering with different word count thresholds
- Five-star aggregation by region
- Error handling for invalid schemas

### Running the Project

#### Prerequisites
```bash
pip install -r requirements.txt
```

Requires: `pyspark>=3.0.0`, `pytest`

#### Basic Execution
```bash
spark-submit src/main.py
```

#### With Custom Parameters
```bash
spark-submit src/main.py \
  --reviews_path data/reviews.csv \
  --places_path data/dim_place.csv \
  --output_path output/five_star_by_region \
  --min_words 10
```

#### Support Different File Formats
```bash
# Read Parquet files
spark-submit src/main.py \
  --reviews_path data/reviews.parquet \
  --places_path data/places.parquet

# Read JSON files
spark-submit src/main.py \
  --reviews_path data/reviews.json \
  --places_path data/places.json
```

#### Run Unit Tests
```bash
pytest tests/test_transform.py -v
```

### Example Output

**Console Output:**
```
=== Logical Plan (explain) ===
[Detailed execution plan...]

=== Result Preview ===
+----------+-----+
|region    |count|
+----------+-----+
|Hanoi     |145  |
|Ho Chi Minh|98  |
|Da Nang   |67   |
+----------+-----+

DONE. Output written to: output/five_star_by_region
```

**Output Files Structure:**
```
output/five_star_by_region/
├── region=Hanoi/
│   ├── part-00000.parquet
│   └── _SUCCESS
├── region=Ho Chi Minh/
│   ├── part-00000.parquet
│   └── _SUCCESS
└── region=Da Nang/
    ├── part-00000.parquet
    └── _SUCCESS
```

### Key Features & Best Practices

✅ **Logging**: Full execution tracking with timestamps and log levels
✅ **Schema Validation**: Catches data quality issues early
✅ **Multi-Format Support**: Handles CSV, Parquet, JSON transparently
✅ **Performance Optimization**: Repartitions data by region before writing for optimal file organization
✅ **Unit Tests**: Comprehensive pytest coverage with fixtures
✅ **Error Handling**: Graceful handling of missing files and invalid schemas
✅ **Documentation**: Clear comments explaining Spark concepts

### Performance Considerations

1. **Lazy Execution**: Transformations don't run until an action is called → efficient optimization
2. **Partitioning**: Output is partitioned by region → faster queries on region filters
3. **Repartitioning**: Data is repartitioned before write → better file distribution
4. **Word Count Filter**: Removes short/spam reviews early → reduces data in subsequent operations
5. **Inner Join**: Only keeps matching records → reduces data size

### Learning Outcomes

This project demonstrates:
- ✨ Spark DataFrame API and lazy evaluation
- ✨ ETL pipeline design (Extract, Transform, Load)
- ✨ Structured data processing and aggregations
- ✨ Proper error handling and logging
- ✨ Testing Spark applications
- ✨ Output optimization with partitioning
