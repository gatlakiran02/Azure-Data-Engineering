import os
import csv
import json
import random
from datetime import datetime, timedelta

def generate_customers(base_path, num_customers=100):
    os.makedirs(os.path.join(base_path, "customers"), exist_ok=True)
    customers_file = os.path.join(base_path, "customers", "customers.csv")
    
    names = ["Emily", "Michael", "Sarah", "James", "David", "Jessica", "John", "Robert", "Patricia", "Linda"]
    surnames = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Miller", "Davis", "Garcia", "Rodriguez", "Wilson"]
    countries = ["USA", "Canada", "UK", "Germany", "France", "Japan"]
    
    with open(customers_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["customer_id", "name", "email", "join_date", "country"])
        
        # Inject standard clients
        for cid in range(1, num_customers + 1):
            name = f"{random.choice(names)} {random.choice(surnames)}"
            email = name.lower().replace(" ", "") + "@example.com"
            # Random date within last 2 years
            days_ago = random.randint(1, 730)
            join_date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
            country = random.choice(countries)
            writer.writerow([cid, name, email, join_date, country])
            
    print(f"Generated customers file: {customers_file}")

def generate_sales(base_path, num_transactions=200):
    os.makedirs(os.path.join(base_path, "sales"), exist_ok=True)
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    sales_file = os.path.join(base_path, "sales", f"sales_{timestamp_str}.json")
    
    transactions = []
    # Create standard transactions
    for txid in range(10001, 10001 + num_transactions):
        # 10% chance of a bad customer id to test integrity
        customer_id = random.randint(1, 110)
        product_id = f"PROD-{random.randint(101, 120)}"
        quantity = random.randint(1, 5)
        # Unit price between $5 and $200
        unit_price = round(random.uniform(5.0, 200.0), 2)
        amount = round(quantity * unit_price, 2)
        
        # Random txn timestamp within last 5 days
        minutes_ago = random.randint(1, 7200)
        txn_time = (datetime.now() - timedelta(minutes=minutes_ago)).strftime("%Y-%m-%d %H:%M:%S")
        
        tx_record = {
            "txn_id": txid,
            "customer_id": customer_id,
            "product_id": product_id,
            "quantity": quantity,
            "amount": amount,
            "txn_timestamp": txn_time
        }
        transactions.append(tx_record)
        
    # INJECT DUPLICATES for Deduplication Silver test
    for _ in range(10):
        dup_tx = random.choice(transactions).copy()
        transactions.append(dup_tx)
        
    # INJECT NULL VALUES to test cleaning rules
    for _ in range(5):
        null_tx = {
            "txn_id": random.randint(20000, 30000),
            "customer_id": None, # Null customer
            "product_id": "PROD-NULL",
            "quantity": None,
            "amount": -50.0, # Negative amount
            "txn_timestamp": None
        }
        transactions.append(null_tx)
        
    with open(sales_file, "w") as f:
        # Write as JSON Lines format (each line is a JSON object), which Spark reads natively
        for tx in transactions:
            f.write(json.dumps(tx) + "\n")
            
    print(f"Generated sales file: {sales_file}")

if __name__ == "__main__":
    import sys
    # If path provided, use it, else default to local raw data directory
    path = sys.argv[1] if len(sys.argv) > 1 else os.path.join("data", "project1", "raw")
    generate_customers(path)
    generate_sales(path)
