#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pyiceberg[sql-sqlite,pyarrow]",
#     "pandas",
#     "duckdb",
# ]
# ///

import duckdb
import os

# Get absolute path to warehouse directory
warehouse_path = os.path.abspath('warehouse')

# Connect to DuckDB
con = duckdb.connect()

# Load the Iceberg extension
con.install_extension('iceberg')
con.load_extension('iceberg')

# Query the table using the table path (DuckDB will use version-hint.text to find the latest metadata)
result = con.sql(f"""
    SELECT 
        airport_code as "Airport",
        date::date as "Date",
        ROUND(celsius, 1) || '°C' as "Temperature"
    FROM iceberg_scan('{warehouse_path}/temps.db/temperatures')
    ORDER BY date 
    LIMIT 5
""")

print("\nFirst 5 rows:")
print(result.fetchdf())
