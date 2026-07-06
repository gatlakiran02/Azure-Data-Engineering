import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name

def run_bronze_stage(spark: SparkSession, raw_path: str, bronze_path: str):
    """
    Reads raw IoT telemetry JSON logs, appends metadata columns, 
    and writes to a Delta table in the Bronze zone.
    """
    print("\n--- Running Project 2 Bronze Stage ---")
    
    print(f"Reading raw telemetry files from: {raw_path}")
    df_raw = spark.read \
        .format("json") \
        .load(raw_path)
        
    df_bronze = df_raw \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_file", input_file_name())
        
    bronze_table_dir = os.path.join(bronze_path, "telemetry")
    print(f"Writing telemetry to Bronze Delta table at: {bronze_table_dir}")
    df_bronze.write \
        .format("delta") \
        .mode("overwrite") \
        .save(bronze_table_dir)
        
    print("Bronze Stage Completed Successfully!")

if __name__ == "__main__":
    from delta import configure_spark_with_delta_pip
    
    builder = SparkSession.builder \
        .appName("Project2_Bronze") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
    spark_sess = configure_spark_with_delta_pip(builder).getOrCreate()
    spark_sess.sparkContext.setLogLevel("WARN")
    
    r_path = os.path.join("data", "project2", "raw")
    b_path = os.path.join("data", "project2", "bronze")
    
    run_bronze_stage(spark_sess, r_path, b_path)
    spark_sess.stop()
