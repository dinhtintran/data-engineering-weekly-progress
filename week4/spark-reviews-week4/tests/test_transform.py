import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from src.transform import filter_min_words, join_place_region, agg_five_star_by_region, validate_schema


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    return (
        SparkSession.builder
        .appName("test-spark-reviews")
        .master("local[1]")
        .getOrCreate()
    )


@pytest.fixture
def sample_reviews_df(spark):
    """Create a sample reviews dataframe."""
    data = [
        ("1", "This is a great place with amazing food and service"),
        ("2", "Good"),
        ("3", "Excellent location and friendly staff highly recommend"),
        ("1", None),  # null value
    ]
    schema = StructType([
        StructField("place identifier", StringType(), True),
        StructField("review text", StringType(), True),
    ])
    return spark.createDataFrame(data, schema=schema)


@pytest.fixture
def sample_places_df(spark):
    """Create a sample places dataframe."""
    data = [
        ("1", "Hanoi"),
        ("2", "Ho Chi Minh"),
        ("3", "Da Nang"),
    ]
    schema = StructType([
        StructField("place identifier", StringType(), True),
        StructField("region", StringType(), True),
    ])
    return spark.createDataFrame(data, schema=schema)


@pytest.fixture
def sample_joined_df(spark, sample_reviews_df, sample_places_df):
    """Create a sample joined dataframe with ratings."""
    data = [
        ("1", "This is a great place with amazing food and service", "Hanoi", 5),
        ("2", "Good", "Ho Chi Minh", 4),
        ("3", "Excellent location and friendly staff highly recommend", "Da Nang", 5),
    ]
    schema = StructType([
        StructField("place identifier", StringType(), True),
        StructField("review text", StringType(), True),
        StructField("region", StringType(), True),
        StructField("star rating", IntegerType(), True),
    ])
    return spark.createDataFrame(data, schema=schema)


class TestValidateSchema:
    """Test schema validation function."""
    
    def test_validate_schema_success(self, sample_reviews_df):
        """Test successful schema validation."""
        validate_schema(sample_reviews_df, ["place identifier", "review text"])
    
    def test_validate_schema_missing_columns(self, sample_reviews_df):
        """Test schema validation with missing columns."""
        with pytest.raises(ValueError, match="Missing required columns"):
            validate_schema(sample_reviews_df, ["place identifier", "nonexistent_column"])


class TestFilterMinWords:
    """Test filter_min_words function."""
    
    def test_filter_min_words_default(self, sample_reviews_df):
        """Test filtering with default 10 words minimum."""
        result = filter_min_words(sample_reviews_df, min_words=10)
        assert result.count() == 2  # Only 2 reviews have >= 10 words
    
    def test_filter_min_words_custom(self, sample_reviews_df):
        """Test filtering with custom word count."""
        result = filter_min_words(sample_reviews_df, min_words=3)
        assert result.count() == 2  # Excluding the short and null review
    
    def test_filter_min_words_invalid_schema(self, sample_places_df):
        """Test filtering with invalid schema."""
        with pytest.raises(ValueError, match="Missing required columns"):
            filter_min_words(sample_places_df)


class TestAggFiveStarByRegion:
    """Test agg_five_star_by_region function."""
    
    def test_agg_five_star_by_region(self, sample_joined_df):
        """Test aggregating five-star reviews by region."""
        result = agg_five_star_by_region(sample_joined_df)
        result_data = result.collect()
        
        assert len(result_data) == 2  # 2 regions with 5-star reviews
        
        # Convert to dict for easier assertion
        result_dict = {row["region"]: row["count"] for row in result_data}
        assert result_dict["Hanoi"] == 1
        assert result_dict["Da Nang"] == 1
    
    def test_agg_five_star_invalid_schema(self, sample_reviews_df):
        """Test aggregation with invalid schema."""
        with pytest.raises(ValueError, match="Missing required columns"):
            agg_five_star_by_region(sample_reviews_df)
