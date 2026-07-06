import os
import shutil
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Import local ETL modules
from generate_data import generate_telemetry
from bronze import run_bronze_stage
from silver import run_silver_stage
from gold import run_gold_stage

def main():
    print("=" * 60)
    # 0. Set paths
    project_root = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(project_root, "data")
    
    raw_path = os.path.join(data_dir, "raw")
    bronze_path = os.path.join(data_dir, "bronze")
    silver_path = os.path.join(data_dir, "silver")
    gold_path = os.path.join(data_dir, "gold")
    
    # 1. Clean previous run files to demonstrate fresh ETL
    print(f"Cleaning previous run directories in: {data_dir}")
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
        
    os.makedirs(data_dir, exist_ok=True)
    
    # 2. Generate Mock Data
    print("Step 1: Generating Raw IoT JSON Mock Data...")
    generate_telemetry(raw_path, num_devices=4, readings_per_device=40)
    
    # 3. Create a Single Spark Session for efficiency
    print("\nStep 2: Starting Spark Session...")
    builder = SparkSession.builder \
        .appName("IoT_Telemetry_Medallion_Pipeline") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.warehouse.dir", os.path.join(project_root, "spark-warehouse"))
        
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    try:
        # 4. Bronze Ingestion
        run_bronze_stage(spark, raw_path, bronze_path)
        
        # 5. Silver Processing
        run_silver_stage(spark, bronze_path, silver_path)
        
        # 6. Gold Analytical Reporting
        run_gold_stage(spark, silver_path, gold_path)
        
        # 7. Verification: Read and display Gold aggregations
        print("\n" + "=" * 10 + " PIPELINE VERIFICATION RESULTS " + "=" * 10)
        
        # Verify Hourly Device Stats
        print("\n=== Sample Rows from Gold Hourly Device Stats Summary ===")
        hourly_stats_df = spark.read.format("delta").load(os.path.join(gold_path, "hourly_device_stats"))
        hourly_stats_df.show(5, truncate=False)
        
        # Verify Maintenance Alerts
        print("\n=== Active Maintenance Alerts (Low Battery < 15%) ===")
        alerts_df = spark.read.format("delta").load(os.path.join(gold_path, "maintenance_alerts"))
        if alerts_df.count() > 0:
            alerts_df.show(5, truncate=False)
        else:
            print("No devices with critical low battery currently.")
        
        # Print counts
        print("\n=== Row Count Reconciliation ===")
        raw_telemetry_count = spark.read.format("json").load(raw_path).count()
        bronze_telemetry_count = spark.read.format("delta").load(os.path.join(bronze_path, "telemetry")).count()
        silver_telemetry_count = spark.read.format("delta").load(os.path.join(silver_path, "telemetry_cleaned")).count()
        
        print(f"Raw Telemetry Records:      {raw_telemetry_count}")
        print(f"Bronze Telemetry Records:    {bronze_telemetry_count}")
        print(f"Silver Telemetry Records:    {silver_telemetry_count} (Sensor anomalies removed)")
        print(f"Anomalies Filtered Out:      {bronze_telemetry_count - silver_telemetry_count}")
        
    finally:
        # Shutdown Spark
        print("\nStopping Spark Session...")
        spark.stop()
        print("=" * 60)

if __name__ == "__main__":
    main()
