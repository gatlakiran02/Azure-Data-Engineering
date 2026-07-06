"""
Azure Data Engineer Training: 01. Python Basics
------------------------------------------------
This script covers core Python fundamentals required for Data Engineering tasks:
1. Variables & Basic Types
2. Data Structures (Lists, Dictionaries, Sets, Tuples)
3. Control Flow & Loops
4. Functions & Lambda Expressions
5. Error Handling
6. Basic File I/O
7. Introductory Pandas Operations
"""

import os
import pandas as pd

def run_section(title):
    print(f"\n{'='*10} {title} {'='*10}")

def main():
    # -------------------------------------------------------------
    # 1. Variables & Basic Types
    # -------------------------------------------------------------
    run_section("1. Variables & Basic Types")
    
    name = "Azure Data Engineer" # String
    experience_years = 5         # Integer
    hourly_rate = 75.50          # Float
    is_active = True             # Boolean
    
    print(f"Name: {name} ({type(name)})")
    print(f"Years: {experience_years} ({type(experience_years)})")
    print(f"Rate: {hourly_rate} ({type(hourly_rate)})")
    print(f"Active: {is_active} ({type(is_active)})")
    
    # String manipulation
    upper_name = name.upper()
    print(f"Upper Case Name: {upper_name}")
    
    # -------------------------------------------------------------
    # 2. Data Structures
    # -------------------------------------------------------------
    run_section("2. Data Structures")
    
    # Lists (Ordered, mutable)
    tech_stack = ["Python", "SQL", "Spark", "ADF"]
    tech_stack.append("Databricks")
    print(f"Tech Stack List: {tech_stack}")
    print(f"First Tech: {tech_stack[0]}, Last Tech: {tech_stack[-1]}")
    
    # Dictionaries (Key-Value pairs, mutable)
    developer = {
        "name": "Sarah",
        "skills": ["ADF", "PySpark", "SQL"],
        "location": "Dallas"
    }
    print(f"Developer Dict: {developer}")
    print(f"Developer Name: {developer['name']}")
    developer["certified"] = True
    print(f"Updated Developer Dict: {developer}")
    
    # Sets (Unordered, unique values)
    regions = {"eastus", "westus", "eastus", "centralus"}
    print(f"Unique Regions Set: {regions}") # Duplicates removed
    
    # Tuples (Ordered, immutable)
    db_config = ("sql-server-prod", 1433)
    print(f"Database Config Tuple: {db_config}")
    
    # -------------------------------------------------------------
    # 3. Control Flow & Loops
    # -------------------------------------------------------------
    run_section("3. Control Flow & Loops")
    
    # Conditional logic
    score = 85
    if score >= 90:
        grade = "Senior"
    elif score >= 70:
        grade = "Mid-Level"
    else:
        grade = "Junior"
    print(f"Developer Level based on score: {grade}")
    
    # For loops
    print("Looping through Tech Stack list:")
    for tech in tech_stack:
        print(f" - {tech}")
        
    # List comprehensions (Powerful Python feature for transforming collections)
    squared_numbers = [x**2 for x in range(5)]
    print(f"Squared Numbers (using list comprehension): {squared_numbers}")
    
    # -------------------------------------------------------------
    # 4. Functions & Lambda Expressions
    # -------------------------------------------------------------
    run_section("4. Functions & Lambda Expressions")
    
    # Regular function
    def calculate_bonus(salary, ratio=0.10):
        """Calculates bonus based on salary and ratio."""
        return salary * ratio
        
    salary = 120000
    bonus = calculate_bonus(salary)
    print(f"Calculated Bonus for {salary}: {bonus}")
    
    # Lambda expression (anonymous function used for simple one-liners)
    celsius_to_fahrenheit = lambda c: (c * 9/5) + 32
    c_temp = 25
    f_temp = celsius_to_fahrenheit(c_temp)
    print(f"Lambda Conversion: {c_temp}°C is {f_temp}°F")
    
    # -------------------------------------------------------------
    # 5. Error Handling (Try-Except)
    # -------------------------------------------------------------
    run_section("5. Error Handling")
    
    try:
        # Intentionally dividing by zero
        result = 10 / 0
    except ZeroDivisionError as e:
        print(f"Caught an expected error: {e}")
    except Exception as e:
        print(f"Caught general error: {e}")
    finally:
        print("This finally block always runs, ideal for closing connections.")
        
    # -------------------------------------------------------------
    # 6. Basic File I/O
    # -------------------------------------------------------------
    run_section("6. File I/O")
    
    file_path = "sample_output.txt"
    # Write to a file
    with open(file_path, "w") as f:
        f.write("Hello Azure Data Engineer!\n")
        f.write("This file is created as part of the Python Basics tutorial.\n")
    print(f"Written text to: {file_path}")
    
    # Read from a file
    with open(file_path, "r") as f:
        content = f.read()
    print("Reading file contents:")
    print(content.strip())
    
    # Clean up file
    if os.path.exists(file_path):
        os.remove(file_path)
        print(f"Cleaned up temporary file: {file_path}")
        
    # -------------------------------------------------------------
    # 7. Introductory Pandas Operations
    # -------------------------------------------------------------
    run_section("7. Pandas Basics")
    
    # Creating a DataFrame from a dictionary
    data = {
        "Pipeline_Name": ["CopySalesData", "IngestTelemetry", "ProcessMetrics", "EmailAlerts"],
        "Status": ["Succeeded", "Failed", "Succeeded", "Succeeded"],
        "Duration_Seconds": [120, 45, 310, 15]
    }
    df = pd.DataFrame(data)
    print("Pandas DataFrame representing Pipeline runs:")
    print(df)
    
    # Basic data analysis in Pandas
    print("\nFiltering DataFrame for Succeeded runs:")
    succeeded_df = df[df["Status"] == "Succeeded"]
    print(succeeded_df)
    
    mean_duration = df["Duration_Seconds"].mean()
    print(f"\nAverage Execution Duration: {mean_duration} seconds")

if __name__ == "__main__":
    main()
