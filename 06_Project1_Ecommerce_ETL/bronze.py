import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name

def run_bronze_stage(spark: SparkSession, raw_path: str, bronze_path: str):
    """
    Ingests raw customer CSV and sales JSON, appends metadata columns, 
    and writes to Delta tables in the Bronze layer.
    """
    print("\n--- Running Project 1 Bronze Stage ---")
    
    # 1. Ingest Customers CSV
    customers_raw_dir = os.path.join(raw_path, "customers")
    print(f"Reading raw customers from: {customers_raw_dir}")
    
    df_customers = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(customers_raw_dir)
        
    # Append Metadata
    df_customers_bronze = df_customers \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_file", input_file_name())
        
    # Write to Bronze Delta
    customers_bronze_dir = os.path.join(bronze_path, "customers")
    print(f"Writing customers to Bronze Delta: {customers_bronze_dir}")
    df_customers_bronze.write \
        .format("delta") \
        .mode("overwrite") \
        .save(customers_bronze_dir)
        
    # 2. Ingest Sales JSON
    sales_raw_dir = os.path.join(raw_path, "sales")
    print(f"Reading raw sales JSON from: {sales_raw_dir}")
    
    df_sales = spark.read \
        .format("json") \
        .load(sales_raw_dir)
        
    # Append Metadata
    df_sales_bronze = df_sales \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_file", input_file_name())
        
    # Write to Bronze Delta
    sales_bronze_dir = os.path.join(bronze_path, "sales")
    print(f"Writing sales transactions to Bronze Delta: {sales_bronze_dir}")
    df_sales_bronze.write \
        .format("delta") \
        .mode("overwrite") \
        .save(sales_bronze_dir)
        
    print("Bronze Stage Completed Successfully!")

if __name__ == "__main__":
    # If executed independently, set up a standalone local SparkSession
    from delta import configure_spark_with_delta_pip
    
    builder = SparkSession.builder \
        .appName("Project1_Bronze") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
    spark_sess = configure_spark_with_delta_pip(builder).getOrCreate()
    spark_sess.sparkContext.setLogLevel("WARN")
    
    r_path = os.path.join("data", "project1", "raw")
    b_path = os.path.join("data", "project1", "bronze")
    
    run_bronze_stage(spark_sess, r_path, b_path)
    spark_sess.stop()
