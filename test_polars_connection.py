#!/usr/bin/env python3
"""
Simple test script to debug Polars connection issue
"""

import os
from dotenv import load_dotenv
import polars as pl

load_dotenv()

SERVER   = os.getenv("MSSQL_SERVER")
DATABASE = os.getenv("MSSQL_DB")
USER     = os.getenv("MSSQL_USER")
PASSWORD = os.getenv("MSSQL_PWD")
PORT     = os.getenv("MSSQL_PORT", "1433")
DRIVER   = os.getenv("MSSQL_DRIVER", "ODBC Driver 17 for SQL Server")
TABLE_NAME = os.getenv("SQL_BENCHMARK_TABLE")

# Test different connection string formats
conn_formats = [
    # Format 1: Original
    f"DRIVER={{{DRIVER}}};SERVER={SERVER},{PORT};DATABASE={DATABASE};UID={USER};PWD={PASSWORD};Trusted_Connection=no;MARS_Connection=yes",
    
    # Format 2: Without MARS
    f"DRIVER={{{DRIVER}}};SERVER={SERVER},{PORT};DATABASE={DATABASE};UID={USER};PWD={PASSWORD};Trusted_Connection=no",
    
    # Format 3: Simplified
    f"DRIVER={{{DRIVER}}};SERVER={SERVER};DATABASE={DATABASE};UID={USER};PWD={PASSWORD}",
    
    # Format 4: Different syntax
    f"Driver={{{DRIVER}}};Server={SERVER};Database={DATABASE};Uid={USER};Pwd={PASSWORD};TrustServerCertificate=yes"
]

simple_query = f"SELECT TOP 10 * FROM {TABLE_NAME}"

print("Testing Polars connection formats...")
print(f"Table: {TABLE_NAME}")
print(f"Server: {SERVER}")
print()

for i, conn_str in enumerate(conn_formats, 1):
    print(f"Format {i}: Testing...")
    try:
        df = pl.read_database(simple_query, conn_str)
        print(f"  ✓ SUCCESS - Retrieved {len(df)} rows")
        print(f"  Columns: {df.columns}")
        break
    except Exception as e:
        print(f"  ✗ FAILED: {type(e).__name__}: {str(e)}")
        if "writer.py" in str(e):
            print(f"    This looks like the arrow-odbc writer issue")
    print()

print("\nTesting versions:")
print(f"Polars: {pl.__version__}")

try:
    import arrow_odbc
    print(f"arrow-odbc: {arrow_odbc.__version__}")
except:
    print("arrow-odbc version not available")

try:
    import pyarrow
    print(f"PyArrow: {pyarrow.__version__}")
except:
    print("PyArrow version not available")
