import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, round, row_number
from pyspark.sql.window import Window

def run_silver_stage(spark: SparkSession, bronze_path: str, silver_path: str):
    """
    Reads Bronze telemetry Delta table, converts temperatures to Celsius, 
    filters out impossible telemetry spikes (anomalies), deduplicates 
    sensor events, and writes to a Silver Delta table.
    """
    print("\n--- Running Project 2 Silver Stage ---")
    
    bronze_table_dir = os.path.join(bronze_path, "telemetry")
    print(f"Reading from Bronze Delta: {bronze_table_dir}")
    df_bronze = spark.read.format("delta").load(bronze_table_dir)
    
    # 1. Cast types, parse dates, and filter out outlier spikes
    print("Casting types, parsing timestamps, and filtering sensor anomalies...")
    df_filtered = df_bronze \
        .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("temperature_f", col("temperature_f").cast("double")) \
        .withColumn("humidity", col("humidity").cast("double")) \
        .withColumn("battery_level", col("battery_level").cast("integer")) \
        .filter(
            col("device_id").isNotNull() & 
            col("timestamp").isNotNull() & 
            (col("temperature_f") > -50.0) & (col("temperature_f") < 150.0) &  # Remove outlier spikes (-500, 999.9)
            (col("humidity") >= 0.0) & (col("humidity") <= 100.0)
        )
        
    # 2. Fahrenheit to Celsius Unit Conversion
    print("Performing temperature unit conversion (Fahrenheit to Celsius)...")
    df_converted = df_filtered \
        .withColumn("temperature_c", round(((col("temperature_f") - 32) * 5 / 9), 2)) \
        .drop("temperature_f")
        
    # 3. Deduplicate readings per device and timestamp
    print("Deduplicating telemetry readings...")
    windowSpec = Window.partitionBy("device_id", "timestamp").orderBy(col("ingestion_timestamp").desc())
    df_dedup = df_converted \
        .withColumn("row_num", row_number().over(windowSpec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")
        
    # 4. Write to Silver Delta Table
    silver_table_dir = os.path.join(silver_path, "telemetry_cleaned")
    print(f"Writing cleansed telemetry to Silver Delta table at: {silver_table_dir}")
    df_dedup.write \
        .format("delta") \
        .mode("overwrite") \
        .save(silver_table_dir)
        
    print("Silver Stage Completed Successfully!")

if __name__ == "__main__":
    from delta import configure_spark_with_delta_pip
    
    builder = SparkSession.builder \
        .appName("Project2_Silver") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
    spark_sess = configure_spark_with_delta_pip(builder).getOrCreate()
    spark_sess.sparkContext.setLogLevel("WARN")
    
    b_path = os.path.join("data", "project2", "bronze")
    s_path = os.path.join("data", "project2", "silver")
    
    run_silver_stage(spark_sess, b_path, s_path)
    spark_sess.stop()
