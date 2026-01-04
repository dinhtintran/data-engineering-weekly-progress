import argparse
from pyspark.sql.functions import col

from utils import create_spark
from transform import filter_min_words, join_place_region, agg_five_star_by_region


def parse_args():
    parser = argparse.ArgumentParser(description="Week4 Spark Mini Project")
    parser.add_argument("--reviews_path", type=str, default="data/reviews.csv")
    parser.add_argument("--places_path", type=str, default="data/dim_place.csv")
    parser.add_argument("--output_path", type=str, default="output/five_star_by_region")
    parser.add_argument("--min_words", type=int, default=10)
    return parser.parse_args()


def main():
    args = parse_args()
    spark = create_spark()

    # 1) Read input CSV
    reviews_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(args.reviews_path)
    )

    places_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(args.places_path)
    )

    # ========== Lazy Execution Explanation ==========
    # Spark chạy theo cơ chế Lazy Execution:
    # - Các bước như filter(), join(), groupBy() là TRANSFORMATION
    #   => Spark CHƯA xử lý dữ liệu ngay
    # - Spark chỉ tạo "kế hoạch" (logical plan)
    # - Chỉ khi gặp ACTION như show(), count(), write()...
    #   => Spark mới thực sự chạy job để tính toán
    #
    # Bạn có thể kiểm tra logical plan bằng explain():
    # (explain() cũng không làm tính toán toàn bộ, nó in kế hoạch)
    # ===============================================

    # 2) Transformations
    filtered_reviews_df = filter_min_words(reviews_df, min_words=args.min_words)

    joined_df = join_place_region(filtered_reviews_df, places_df)

    result_df = agg_five_star_by_region(joined_df)

    # In kế hoạch thực thi (không phải action tính toán full data)
    print("\n=== Logical Plan (explain) ===")
    result_df.explain(True)

    # 3) Action: show()
    print("\n=== Result Preview ===")
    result_df.orderBy(col("count").desc()).show(truncate=False)

    # 4) Action: write parquet partitionBy region
    (
        result_df.write
        .mode("overwrite")
        .partitionBy("region")
        .parquet(args.output_path)
    )

    print(f"\nDONE. Output written to: {args.output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
