#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pyiceberg[sql-sqlite,pyarrow]",
#     "pandas",
#     "duckdb",
# ]
# ///

import os
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from typing import Optional

def load_temperature_data(csv_path: str) -> pd.DataFrame:
    """Load temperature data from CSV file."""
    return pd.read_csv(csv_path, parse_dates=['date'])

def create_arrow_table(df: pd.DataFrame) -> pa.Table:
    """Convert pandas DataFrame to PyArrow table with required schema."""
    schema = pa.schema([
        pa.field('airport_code', pa.string(), nullable=False),
        pa.field('date', pa.timestamp('us'), nullable=False),
        pa.field('celsius', pa.float64(), nullable=False)
    ])
    return pa.Table.from_pandas(df, schema=schema)

def write_version_hint(metadata_dir: str) -> None:
    """Write version-hint.text with the full metadata version.
    
    This is the pointer to the latest metadata file that duckdb will leverage when reading.
    DuckDB expects this file, but the pyiceberg library doesn't create it as documented in:
    https://github.com/duckdb/duckdb-iceberg/issues/29
    """
    metadata_files = [f for f in os.listdir(metadata_dir) if f.endswith('.metadata.json')]
    if metadata_files:
        latest_file = sorted(metadata_files, key=lambda x: int(x.split('-')[0]))[-1]
        version_hint = latest_file.rsplit('.metadata.json', 1)[0]
        print(f"Writing version hint {version_hint} to version-hint.text")
        with open(os.path.join(metadata_dir, 'version-hint.text'), 'w') as f:
            f.write(version_hint)

def create_iceberg_table(db_name: str = 'temps', 
                        table_name: str = 'temperatures',
                        csv_path: Optional[str] = None) -> None:
    """Create an Iceberg table and load data from CSV."""
    catalog = load_catalog(
        'local',
        **{
            "type": "sql",
            "uri": "sqlite:///warehouse/iceberg.db",
            "warehouse": "warehouse",
            "catalog-impl": "sqlite"
        }
    )

    # Create namespace if it doesn't exist
    try:
        catalog.create_namespace(db_name)
        print(f"Created namespace: {db_name}")
    except NamespaceAlreadyExistsError:
        print(f"Namespace already exists: {db_name}")

    # Create or get table
    try:
        table = catalog.load_table((db_name, table_name))
        print(f"Loaded existing table: {(db_name, table_name)}")
    except NoSuchTableError:
        table = catalog.create_table(
            (db_name, table_name),
            schema=pa.schema([
                pa.field('airport_code', pa.string(), nullable=False),
                pa.field('date', pa.timestamp('us'), nullable=False),
                pa.field('celsius', pa.float64(), nullable=False)
            ]),
            properties={
                'format-version': '2',
                'write.parquet.compression-codec': 'zstd'
            }
        )
        print(f"Created new table: {(db_name, table_name)}")

    # Load data if table is empty and CSV path provided
    if table.current_snapshot() is None and csv_path:
        print("Loading data into empty table")
        df = load_temperature_data(csv_path)
        pa_table = create_arrow_table(df)
        table.append(pa_table)

        # Write version-hint.text for DuckDB compatibility
        metadata_dir = os.path.join('warehouse', f'{db_name}.db', table_name, 'metadata')
        write_version_hint(metadata_dir)
        print("Loaded data into empty table")
    else:
        print("Table already contains data, skipping data load")

    print(f"Table: {(db_name, table_name)}")
    print(f"Schema: {table.schema()}")

def main():
    """Create Iceberg table and load temperature data."""
    # Create warehouse directory if it doesn't exist
    os.makedirs("warehouse", exist_ok=True)
    create_iceberg_table(csv_path='data/temperatures.csv')

if __name__ == '__main__':
    main()
