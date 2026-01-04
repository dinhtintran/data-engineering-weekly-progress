from pyspark.sql import DataFrame
from pyspark.sql.functions import col, size, split 

def filter_min_words(reviews_df: DataFrame, min_words: int = 10) -> DataFrame:
    return reviews_df.filter(size(split(col("reveiw_text"), " ")) >= min_words)

def join_place_region(filtered_reviews_df: DataFrame, places_df: DataFrame) -> DataFrame:
    return filtered_reviews_df.join(places_df, on="place_id", how="inner")

def agg_five_star_by_region(joined_df: DataFrame) -> DataFrame:
    five_star = joined_df.filter(col("rating") == 5)
    return five_star.groupBy("region").count()