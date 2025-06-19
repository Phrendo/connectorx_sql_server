#!/usr/bin/env python3
import os
from dotenv import load_dotenv 
import connectorx as cx
import polars as pl

# ── CONFIG ──────────────────────────────────────────────────────────────────────
load_dotenv()
SERVER   = os.getenv("MSSQL_SERVER")
DATABASE = os.getenv("MSSQL_DB")
USER     = os.getenv("MSSQL_USER")
PASSWORD = os.getenv("MSSQL_PWD")
PORT     = os.getenv("MSSQL_PORT",   "1433")
DRIVER   = os.getenv("MSSQL_DRIVER", "ODBC Driver 17 for SQL Server")

# ── This URI is for connectorx
CONN_URI  = (
    f"mssql://{USER}:{PASSWORD}@{SERVER}:{PORT}/{DATABASE}"
    f"?driver={DRIVER.replace(' ', '+')}"
)
# ── This Connstring is for polars
CONN_ODBC = (
    f"DRIVER={{{DRIVER}}};SERVER={SERVER},{PORT};"
    f"DATABASE={DATABASE};UID={USER};PWD={PASSWORD};"
    "Trusted_Connection=no;MARS_Connection=yes"
)

# our simple query
sql = f"SELECT name, create_date FROM sys.databases"

# Direct from connectorx
df = cx.read_sql(CONN_URI, sql, return_type="pandas")
print(df)

# Direct from polars
df = pl.read_database(sql, CONN_ODBC)
print(df)

# From connectorx to polars arrow format -- this will be the fastest for large datasets.
df = pl.from_arrow(cx.read_sql(CONN_URI, sql, return_type="arrow"))
print(df)
