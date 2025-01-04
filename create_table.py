#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pyiceberg[sql-sqlite,pyarrow]",
#     "pandas",
# ]
# ///

from pyiceberg.catalog import load_catalog, NamespaceAlreadyExistsError
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import (
    TimestampType,
    StringType,
    DoubleType,
)
from pyiceberg.table import TableProperties
import pandas as pd
import pyarrow as pa
import os

# Define the schema for our temperatures table
schema = Schema(
    # airport_code,date,celsius
    NestedField(1, "airport_code", StringType(), required=True),
    NestedField(2, "date", TimestampType(), required=True),
    NestedField(3, "celsius", DoubleType(), required=True),
)

# Create warehouse directory if it doesn't exist
os.makedirs("warehouse", exist_ok=True)

# Create a catalog
catalog = load_catalog(
    "local",  # catalog name
    **{
        "type": "sql",
        "uri": "sqlite:///warehouse/iceberg.db",
        "warehouse": "warehouse",  # directory to store tables
        "metadata-location-format": "metadata/metadata.json"  # standard metadata location
    }
)

# Create namespace if it doesn't exist
try:
    catalog.create_namespace("temps")
except NamespaceAlreadyExistsError:
    pass

# Create the table if it doesn't exist
table = catalog.create_table_if_not_exists(
    identifier="temps.temperatures",  # namespace.table_name
    schema=schema,
    properties={
        TableProperties.FORMAT_VERSION: "2",  # using Iceberg format v2
    }
)

# Load data if table is empty
if table.current_snapshot() is None:
    print("Loading data into empty table")
    df = pd.read_csv('data/temperatures.csv', parse_dates=['date'])
    
    # Convert pandas DataFrame to PyArrow table with microsecond precision and required fields
    schema = pa.schema([
        pa.field('airport_code', pa.string(), nullable=False),
        pa.field('date', pa.timestamp('us'), nullable=False),
        pa.field('celsius', pa.float64(), nullable=False)
    ])
    pa_table = pa.Table.from_pandas(df, schema=schema)

    # Append data to the table
    table.append(pa_table)

    # Write version-hint.text with the full metadata version (everything before .metadata.json)
    # this is the pointer to the latest metadata file that duckdb will leverage when reading
    # duckdb expects this file, but the pyiceberg library doesn't create it as documented in this duckdb issue:
    # https://github.com/duckdb/duckdb-iceberg/issues/29
    metadata_dir = os.path.join('warehouse', 'temps.db', 'temperatures', 'metadata')
    metadata_files = [f for f in os.listdir(metadata_dir) if f.endswith('.metadata.json')]
    if metadata_files:
        # Sort by version number (the prefix before the first dash)
        latest_file = sorted(metadata_files, key=lambda x: int(x.split('-')[0]))[-1]
        # Get everything before .metadata.json
        version_hint = latest_file.rsplit('.metadata.json', 1)[0]
        print(f"Writing version hint {version_hint} to version-hint.text")
        with open(os.path.join(metadata_dir, 'version-hint.text'), 'w') as f:
            f.write(version_hint)

    print("Loaded data into empty table")
else:
    print("Table already contains data, skipping data load")

print(f"Table: {table.identifier}")
print(f"Schema: {table.schema()}")
