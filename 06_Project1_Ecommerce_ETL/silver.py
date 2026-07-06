import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, row_number, trim, lower
from pyspark.sql.window import Window

def run_silver_stage(spark: SparkSession, bronze_path: str, silver_path: str):
    """
    Cleans Bronze tables (deduplicates transactions, casts types, handles nulls),
    joins sales transactions with customers, and writes the enriched records 
    to the Silver zone.
    """
    print("\n--- Running Project 1 Silver Stage ---")
    
    customers_bronze_dir = os.path.join(bronze_path, "customers")
    sales_bronze_dir = os.path.join(bronze_path, "sales")
    
    print(f"Reading from Bronze Customers: {customers_bronze_dir}")
    df_customers = spark.read.format("delta").load(customers_bronze_dir)
    
    print(f"Reading from Bronze Sales: {sales_bronze_dir}")
    df_sales = spark.read.format("delta").load(sales_bronze_dir)
    
    # 1. Clean & Standardize Customers
    print("Cleaning Customer profiles...")
    df_customers_clean = df_customers \
        .withColumn("name", trim(col("name"))) \
        .withColumn("email", lower(trim(col("email")))) \
        .withColumn("country", trim(col("country"))) \
        .select(
            col("customer_id").alias("c_customer_id"), 
            col("name").alias("customer_name"), 
            col("email").alias("customer_email"), 
            col("join_date"), 
            col("country")
        )
        
    # 2. Clean, Cast & Deduplicate Sales
    print("Deduplicating and filtering Sales transactions...")
    
    # Cast types and filter nulls/negatives
    df_sales_cast = df_sales \
        .withColumn("txn_timestamp", to_timestamp(col("txn_timestamp"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("amount", col("amount").cast("double")) \
        .withColumn("quantity", col("quantity").cast("integer")) \
        .filter(
            col("txn_id").isNotNull() & 
            col("customer_id").isNotNull() & 
            (col("amount") > 0.0) & 
            col("quantity").isNotNull()
        )
        
    # Deduplication using Window function (keep latest txn by ingestion timestamp)
    windowSpec = Window.partitionBy("txn_id").orderBy(col("ingestion_timestamp").desc())
    df_sales_dedup = df_sales_cast \
        .withColumn("row_num", row_number().over(windowSpec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")
        
    # 3. Join Sales and Customers
    print("Joining Customer profiles and Transaction logs...")
    df_enriched = df_sales_dedup.join(
        df_customers_clean, 
        on=df_sales_dedup.customer_id == df_customers_clean.c_customer_id, 
        how="inner"
    ).drop("c_customer_id")
    
    # 4. Write to Silver Delta Table
    sales_silver_dir = os.path.join(silver_path, "enriched_sales")
    print(f"Writing enriched sales to Silver Delta: {sales_silver_dir}")
    df_enriched.write \
        .format("delta") \
        .mode("overwrite") \
        .save(sales_silver_dir)
        
    print("Silver Stage Completed Successfully!")

if __name__ == "__main__":
    from delta import configure_spark_with_delta_pip
    
    builder = SparkSession.builder \
        .appName("Project1_Silver") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
    spark_sess = configure_spark_with_delta_pip(builder).getOrCreate()
    spark_sess.sparkContext.setLogLevel("WARN")
    
    b_path = os.path.join("data", "project1", "bronze")
    s_path = os.path.join("data", "project1", "silver")
    
    run_silver_stage(spark_sess, b_path, s_path)
    spark_sess.stop()
