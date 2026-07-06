import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum, count, round, desc

def run_gold_stage(spark: SparkSession, silver_path: str, gold_path: str):
    """
    Reads Silver enriched sales data, computes analytical business aggregates, 
    and writes them to the Gold layer (optimized for reporting).
    """
    print("\n--- Running Project 1 Gold Stage ---")
    
    enriched_sales_dir = os.path.join(silver_path, "enriched_sales")
    print(f"Reading enriched sales from Silver: {enriched_sales_dir}")
    df_silver = spark.read.format("delta").load(enriched_sales_dir)
    
    # 1. Gold Aggregate 1: Daily Sales Summary (by Date and Country)
    print("Computing Daily Sales Summary...")
    df_daily_sales = df_silver \
        .withColumn("txn_date", to_date(col("txn_timestamp"))) \
        .groupBy("txn_date", "country") \
        .agg(
            round(sum("amount"), 2).alias("total_revenue"),
            sum("quantity").alias("total_quantity"),
            count("txn_id").alias("transaction_count")
        ) \
        .orderBy("txn_date", desc("total_revenue"))
        
    # Write to Gold
    daily_sales_gold_dir = os.path.join(gold_path, "daily_sales")
    print(f"Writing daily sales to Gold Delta: {daily_sales_gold_dir}")
    df_daily_sales.write \
        .format("delta") \
        .mode("overwrite") \
        .save(daily_sales_gold_dir)
        
    # 2. Gold Aggregate 2: Customer Lifetime Value (LTV) / Spending metrics
    print("Computing Customer Revenue metrics...")
    df_customer_spending = df_silver \
        .groupBy("customer_name", "customer_email", "country") \
        .agg(
            round(sum("amount"), 2).alias("total_spent"),
            sum("quantity").alias("items_purchased"),
            count("txn_id").alias("total_transactions")
        ) \
        .orderBy(desc("total_spent"))
        
    # Write to Gold
    customer_revenue_gold_dir = os.path.join(gold_path, "customer_spending")
    print(f"Writing customer spending to Gold Delta: {customer_revenue_gold_dir}")
    df_customer_spending.write \
        .format("delta") \
        .mode("overwrite") \
        .save(customer_revenue_gold_dir)
        
    print("Gold Stage Completed Successfully!")

if __name__ == "__main__":
    from delta import configure_spark_with_delta_pip
    
    builder = SparkSession.builder \
        .appName("Project1_Gold") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
    spark_sess = configure_spark_with_delta_pip(builder).getOrCreate()
    spark_sess.sparkContext.setLogLevel("WARN")
    
    s_path = os.path.join("data", "project1", "silver")
    g_path = os.path.join("data", "project1", "gold")
    
    run_gold_stage(spark_sess, s_path, g_path)
    spark_sess.stop()
