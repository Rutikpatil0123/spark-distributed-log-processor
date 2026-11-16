from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def create_spark_session():
    """ Initialize and retrun local SparkSession. """
    return  SparkSession.builder.appName("DistributedLogAnalytics") \
    .master("local[*]").config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "127.0.0.1").getOrCreate()

def transform_logs(df):
    """ Applies business treansfromations lazily
    """
    filtered_df = df.filter(F.col("status_code") >= 400).select("ip_address", "timestamp", "endpoint", "status_code")

    agg_df = filtered_df.groupBy("endpoint", "status_code").count().withColumnRenamed("count", "error_count").orderBy(F.col("error_count").desc())
    return agg_df

def process_logs(spark, input_path, output_path):
    """ Reads raw logs, calls transfrmations logic
    and witres out optimsed parquet files. """

    # Reads raw CSV
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)

    print("--- Logical plan brfore transformations ---")
    df.explain()

    # Apply Transformations

    agg_df = transform_logs(df)
    
    print("--- Optimized physical plan before action ---")
    agg_df.explain()

    # --- ADD THESE DEBUG LINES ---
    print("\n--- DEBUG: Preview of Aggregated Data ---")
    agg_df.show(5, truncate=False)
    print(f"Total rows to be written: {agg_df.count()}")

    # Action

    agg_df.write.mode("overwrite").partitionBy("status_code").parquet(output_path)

    print(f"Successfully processed logs and saved to {output_path}")

if __name__ == "__main__":
    spark = create_spark_session()

    # Define input and output paths

    input_path = "data/raw_logs.csv"
    output_path = "output/processed_logs"
    process_logs(spark, input_path, output_path)
    spark.stop()