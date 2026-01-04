import logging
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame

# Configure logging
def setup_logger(name: str, log_level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    
    # Console handler
    handler = logging.StreamHandler()
    handler.setLevel(log_level)
    
    # Formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    
    if not logger.handlers:
        logger.addHandler(handler)
    
    return logger

def read_data(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Read data from CSV, Parquet, or JSON file.
    
    Args:
        spark: SparkSession instance
        file_path: Path to the data file
        
    Returns:
        DataFrame
    """
    logger = setup_logger(__name__)
    path = Path(file_path)
    
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    suffix = path.suffix.lower()
    logger.info(f"Reading {suffix} file: {file_path}")
    
    if suffix == ".csv":
        return (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(file_path)
        )
    elif suffix == ".parquet":
        return spark.read.parquet(file_path)
    elif suffix == ".json":
        return spark.read.json(file_path)
    else:
        raise ValueError(f"Unsupported file format: {suffix}. Supported: .csv, .parquet, .json")

def create_spark(app_name: str = "spark-reviews-week4") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )
    return spark