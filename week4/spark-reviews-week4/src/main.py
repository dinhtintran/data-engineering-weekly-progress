import argparse
from pyspark.sql.functions import col

from utils import create_spark, setup_logger, read_data
from transform import filter_min_words, join_place_region, agg_five_star_by_region

logger = setup_logger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="Week 4 Spark Mini Project")
    parser.add_argument("--reviews_path", type=str, default="data/reviews.csv")
    parser.add_argument("--places_path", type=str, default="data/dim_place.csv")
    parser.add_argument("--output_path", type=str, default="output/five_star_by_region")
    parser.add_argument("--min_words", type=int, default=10)
    return parser.parse_args()


def main():
    args = parse_args()
    logger.info(f"Starting Spark job with args: {args}")
    
    spark = create_spark()
    logger.info("Spark session created successfully")

    # 1) Read input data (CSV, Parquet, or JSON)
    logger.info(f"Reading reviews from: {args.reviews_path}")
    reviews_df = read_data(spark, args.reviews_path)
    logger.info(f"Reviews data loaded: {reviews_df.count()} rows")

    logger.info(f"Reading places from: {args.places_path}")
    places_df = read_data(spark, args.places_path)
    logger.info(f"Places data loaded: {places_df.count()} rows")

    # ========== Lazy Execution Explanation ==========
    # Spark runs using Lazy Execution mechanism:
    # - Steps like filter(), join(), groupBy() are TRANSFORMATIONS
    #   => Spark does NOT process data immediately
    # - Spark only creates a "plan" (logical plan)
    # - Only when encountering ACTION like show(), count(), write()...
    #   => Spark actually runs the job to compute
    #
    # You can check the logical plan using explain():
    # (explain() also doesn't compute full data, it prints the plan)
    # ===============================================

    # 2) Transformations
    logger.info(f"Filtering reviews with minimum {args.min_words} words...")
    filtered_reviews_df = filter_min_words(reviews_df, min_words=args.min_words)
    logger.info(f"Filtered reviews: {filtered_reviews_df.count()} rows")

    logger.info("Joining reviews with place regions...")
    joined_df = join_place_region(filtered_reviews_df, places_df)
    logger.info(f"Joined data: {joined_df.count()} rows")

    logger.info("Aggregating five-star reviews by region...")
    result_df = agg_five_star_by_region(joined_df)

    # Print execution plan (not an action that computes full data)
    logger.info("Explaining query plan...")
    print("\n=== Logical Plan (explain) ===")
    result_df.explain(True)

    # 3) Action: show()
    logger.info("Showing results preview...")
    print("\n=== Result Preview ===")
    result_df.orderBy(col("count").desc()).show(truncate=False)

    # 4) Action: write parquet partitionBy region (optimized with repartition)
    logger.info("Repartitioning by region for optimal output...")
    result_df_repartitioned = result_df.repartition("region")
    
    logger.info(f"Writing output to: {args.output_path}")
    (
        result_df_repartitioned.write
        .mode("overwrite")
        .partitionBy("region")
        .parquet(args.output_path)
    )

    logger.info(f"DONE. Output written to: {args.output_path}")
    spark.stop()


if __name__ == "__main__":
    main()
