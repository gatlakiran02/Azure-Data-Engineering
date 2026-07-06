import os
import shutil
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Import local ETL modules
from generate_data import generate_customers, generate_sales
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
    print("Step 1: Generating Raw CSV and JSON Mock Data...")
    generate_customers(raw_path)
    generate_sales(raw_path)
    
    # 3. Create a Single Spark Session for efficiency
    print("\nStep 2: Starting Spark Session...")
    builder = SparkSession.builder \
        .appName("ECommerce_Medallion_Pipeline") \
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
        
        # Verify Daily Sales
        print("\n=== Sample Rows from Gold Daily Sales Summary ===")
        daily_sales_df = spark.read.format("delta").load(os.path.join(gold_path, "daily_sales"))
        daily_sales_df.show(5, truncate=False)
        
        # Verify Customer Spending
        print("\n=== Top Customers by Spend (Gold Layer) ===")
        cust_spending_df = spark.read.format("delta").load(os.path.join(gold_path, "customer_spending"))
        cust_spending_df.show(5, truncate=False)
        
        # Print counts
        print("\n=== Row Count Reconciliation ===")
        raw_cust_count = spark.read.format("csv").option("header", "true").load(os.path.join(raw_path, "customers")).count()
        raw_sales_count = spark.read.format("json").load(os.path.join(raw_path, "sales")).count()
        bronze_sales_count = spark.read.format("delta").load(os.path.join(bronze_path, "sales")).count()
        silver_sales_count = spark.read.format("delta").load(os.path.join(silver_path, "enriched_sales")).count()
        
        print(f"Raw Customers Generated:    {raw_cust_count}")
        print(f"Raw Sales Transactions:     {raw_sales_count}")
        print(f"Bronze Transactions Logged: {bronze_sales_count}")
        print(f"Silver Transactions Cleaned: {silver_sales_count} (Duplicates & Nulls removed)")
        print(f"Reconciliation Success: {raw_sales_count >= silver_sales_count}")
        
    finally:
        # Shutdown Spark
        print("\nStopping Spark Session...")
        spark.stop()
        print("=" * 60)

if __name__ == "__main__":
    main()
