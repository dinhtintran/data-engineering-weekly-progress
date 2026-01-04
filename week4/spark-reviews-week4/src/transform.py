from pyspark.sql import DataFrame
from pyspark.sql.functions import col, size, split
from utils import setup_logger

logger = setup_logger(__name__)

def validate_schema(df: DataFrame, required_columns: list) -> None:
    """Validate that dataframe has all required columns."""
    missing_cols = set(required_columns) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    logger.info(f"Schema validation passed. Columns: {df.columns}")

def filter_min_words(reviews_df: DataFrame, min_words: int = 10) -> DataFrame:
    logger.info(f"Filtering reviews with minimum {min_words} words")
    validate_schema(reviews_df, ["review text"])
    
    # Check for null values
    null_count = reviews_df.filter(col("review text").isNull()).count()
    if null_count > 0:
        logger.warning(f"Found {null_count} null values in 'review text' column")
    
    return reviews_df.filter(size(split(col("review text"), " ")) >= min_words)

def join_place_region(filtered_reviews_df: DataFrame, places_df: DataFrame) -> DataFrame:
    logger.info("Joining reviews with places")
    validate_schema(filtered_reviews_df, ["place identifier"])
    validate_schema(places_df, ["place identifier", "region"])
    
    return filtered_reviews_df.join(places_df, on="place identifier", how="inner")

def agg_five_star_by_region(joined_df: DataFrame) -> DataFrame:
    logger.info("Aggregating five-star reviews by region")
    validate_schema(joined_df, ["star rating", "region"])
    
    five_star = joined_df.filter(col("star rating") == 5)
    logger.info(f"Found {five_star.count()} five-star reviews")
    
    return five_star.groupBy("region").count()