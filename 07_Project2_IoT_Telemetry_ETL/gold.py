import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_trunc, avg, min, count, round, desc

def run_gold_stage(spark: SparkSession, silver_path: str, gold_path: str):
    """
    Reads Silver cleansed telemetry data, computes hourly average device stats, 
    flags devices with critically low battery levels, and writes to Gold tables.
    """
    print("\n--- Running Project 2 Gold Stage ---")
    
    silver_table_dir = os.path.join(silver_path, "telemetry_cleaned")
    print(f"Reading cleansed telemetry from: {silver_table_dir}")
    df_silver = spark.read.format("delta").load(silver_table_dir)
    
    # 1. Gold Aggregate 1: Hourly Device Summary
    print("Computing Hourly Average Device Statistics...")
    df_hourly_stats = df_silver \
        .withColumn("hour", date_trunc("hour", col("timestamp"))) \
        .groupBy("device_id", "hour") \
        .agg(
            round(avg("temperature_c"), 2).alias("avg_temp_c"),
            round(avg("humidity"), 2).alias("avg_humidity"),
            min("battery_level").alias("min_battery_level"),
            count("timestamp").alias("readings_count")
        ) \
        .orderBy("device_id", "hour")
        
    # Write to Gold
    hourly_stats_gold_dir = os.path.join(gold_path, "hourly_device_stats")
    print(f"Writing hourly stats to Gold Delta table: {hourly_stats_gold_dir}")
    df_hourly_stats.write \
        .format("delta") \
        .mode("overwrite") \
        .save(hourly_stats_gold_dir)
        
    # 2. Gold Aggregate 2: Maintenance Alerts (Battery < 15%)
    print("Identifying devices with low battery levels...")
    df_alerts = df_silver \
        .filter(col("battery_level") < 15) \
        .select("device_id", "timestamp", "battery_level") \
        .orderBy("device_id", desc("timestamp"))
        
    # Write to Gold
    maintenance_gold_dir = os.path.join(gold_path, "maintenance_alerts")
    print(f"Writing maintenance alerts to Gold Delta table: {maintenance_gold_dir}")
    df_alerts.write \
        .format("delta") \
        .mode("overwrite") \
        .save(maintenance_gold_dir)
        
    print("Gold Stage Completed Successfully!")

if __name__ == "__main__":
    from delta import configure_spark_with_delta_pip
    
    builder = SparkSession.builder \
        .appName("Project2_Gold") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
    spark_sess = configure_spark_with_delta_pip(builder).getOrCreate()
    spark_sess.sparkContext.setLogLevel("WARN")
    
    s_path = os.path.join("data", "project2", "silver")
    g_path = os.path.join("data", "project2", "gold")
    
    run_gold_stage(spark_sess, s_path, g_path)
    spark_sess.stop()
