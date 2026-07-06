"""
Azure Data Engineer Training: 03. PySpark Fundamentals
------------------------------------------------------
This script demonstrates basic Spark operations using the PySpark DataFrame API
and Delta Lake. To run this script locally, ensure you have:
1. Java 8 or 11 JDK installed (required by Apache Spark)
2. pyspark and delta-spark library installed (pip install pyspark delta-spark)

Key concepts covered:
- SparkSession creation (with Delta support)
- Creating & transforming DataFrames
- Joins and Aggregations
- Spark SQL queries
- Reading & Writing Delta Tables
"""

import sys
import os

# Set HADOOP_HOME for Windows environments using downloaded winutils
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
os.environ["HADOOP_HOME"] = os.path.join(parent_dir, "hadoop")
os.environ["PATH"] = os.path.join(parent_dir, "hadoop", "bin") + os.pathsep + os.environ.get("PATH", "")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, lit, when, avg, sum, count, desc, to_date
    from delta import configure_spark_with_delta_pip
except ImportError:
    print("Warning: pyspark or delta-spark not installed. Run: pip install pyspark delta-spark")
    sys.exit(1)

def run_section(title):
    print(f"\n{'='*15} {title} {'='*15}")

def main():
    # -------------------------------------------------------------
    # 1. Initialize SparkSession with Delta Lake support
    # -------------------------------------------------------------
    run_section("1. Initializing SparkSession")
    
    # Configure SparkSession to use Delta Lake
    builder = SparkSession.builder \
        .appName("PySparkBasics") \
        .master("local[1]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"Spark version: {spark.version}")
    
    # -------------------------------------------------------------
    # 2. Creating DataFrames
    # -------------------------------------------------------------
    run_section("2. Creating DataFrames")
    
    # Sample customer data
    customers_data = [
        (1, "Alice", "Premium", "2023-01-10"),
        (2, "Bob", "Standard", "2023-03-15"),
        (3, "Charlie", "Premium", "2022-11-05"),
        (4, "David", "Standard", "2023-06-20"),
    ]
    schema = ["customer_id", "name", "tier", "signup_date"]
    
    df_customers = spark.createDataFrame(customers_data, schema=schema)
    print("Customers DataFrame:")
    df_customers.show()
    
    # Print schema
    print("Schema of Customers DataFrame:")
    df_customers.printSchema()
    
    # -------------------------------------------------------------
    # 3. DataFrame Transformations & Operations
    # -------------------------------------------------------------
    run_section("3. DataFrame Transformations")
    
    # A. Select, Filter, and Alias
    print("Selecting specific columns and filtering Tier = 'Premium':")
    df_premium = df_customers.select(
        col("customer_id"), 
        col("name").alias("customer_name")
    ).filter(col("tier") == "Premium")
    df_premium.show()
    
    # B. Adding and Modifying Columns (withColumn, when-otherwise)
    print("Adding a new column 'is_active' and parsing 'signup_date' to DateType:")
    df_transformed = df_customers \
        .withColumn("signup_date", to_date(col("signup_date"), "yyyy-MM-dd")) \
        .withColumn("is_active", lit(True)) \
        .withColumn("segment", when(col("tier") == "Premium", "High-Value").otherwise("Standard-Value"))
    df_transformed.show()
    df_transformed.printSchema()
    
    # -------------------------------------------------------------
    # 4. Joins and Aggregations
    # -------------------------------------------------------------
    run_section("4. Joins & Aggregations")
    
    # Sample sales transactions data
    sales_data = [
        (101, 1, 250.50, "2023-05-12"),
        (102, 2, 45.00, "2023-05-13"),
        (103, 1, 120.00, "2023-05-14"),
        (104, 3, 500.00, "2023-05-15"),
    ]
    sales_schema = ["txn_id", "customer_id", "amount", "txn_date"]
    df_sales = spark.createDataFrame(sales_data, schema=sales_schema)
    
    print("Sales Transactions DataFrame:")
    df_sales.show()
    
    # Inner Join
    print("Joining Customers and Sales DataFrames:")
    df_joined = df_customers.join(df_sales, on="customer_id", how="inner")
    df_joined.show()
    
    # Aggregation
    print("Aggregating Total and Average Sales Amount by Customer Tier:")
    df_agg = df_joined.groupBy("tier").agg(
        sum("amount").alias("total_sales"),
        avg("amount").alias("avg_sales"),
        count("txn_id").alias("transaction_count")
    ).orderBy(desc("total_sales"))
    df_agg.show()
    
    # -------------------------------------------------------------
    # 5. Running Spark SQL Queries
    # -------------------------------------------------------------
    run_section("5. Spark SQL Queries")
    
    # Registering DataFrames as Temporary Views
    df_customers.createOrReplaceTempView("view_customers")
    df_sales.createOrReplaceTempView("view_sales")
    
    # Query using SQL syntax
    sql_query = """
        SELECT c.name, SUM(s.amount) as total_spent
        FROM view_customers c
        JOIN view_sales s ON c.customer_id = s.customer_id
        GROUP BY c.name
        ORDER BY total_spent DESC
    """
    df_sql_result = spark.sql(sql_query)
    print("Result of SQL Query:")
    df_sql_result.show()
    
    # -------------------------------------------------------------
    # 6. Reading & Writing Delta Tables (Simulating ADB/Medallion)
    # -------------------------------------------------------------
    run_section("6. Delta Lake Writing & Reading")
    
    delta_path = "data/temp_delta_customers"
    
    print(f"Writing customer data to Delta table at: {delta_path}")
    df_customers.write.format("delta").mode("overwrite").save(delta_path)
    
    print("\nReading customer data back from Delta path:")
    df_delta_read = spark.read.format("delta").load(delta_path)
    df_delta_read.show()
    
    # Show Delta Table History
    try:
        from delta.tables import DeltaTable
        deltaTable = DeltaTable.forPath(spark, delta_path)
        print("\nDelta Table History (Time Travel log):")
        deltaTable.history().select("version", "timestamp", "operation", "operationParameters").show(truncate=False)
    except Exception as e:
        print(f"Unable to show Delta history (check delta libraries): {e}")
        
    # Clean up local Delta folders
    import shutil
    if os.path.exists(delta_path):
        shutil.rmtree(delta_path)
        print(f"\nCleaned up local Delta folder: {delta_path}")
        
    # Shutdown Spark Session
    spark.stop()
    print("\nSpark Session stopped successfully.")

if __name__ == "__main__":
    main()
