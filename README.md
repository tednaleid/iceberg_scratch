# Iceberg Table with DuckDB Example

This project demonstrates how to create an Apache Iceberg table with pyiceberg and query using both DuckDB's SQL interface and Python. It includes sample temperature data from various airports and shows how to properly handle Iceberg metadata for DuckDB compatibility.

## Requirements

- [uv](https://github.com/astral-sh/uv) package manager
    - manages python version and dependencies
- DuckDB 
    - `brew install duckdb`

## Project Structure

```
.
├── README.md
├── create_table.py    # Creates Iceberg table and loads data
├── query.sql          # SQL queries using DuckDB CLI
├── query_table.py     # Python script using DuckDB API
└── data/
    └── temperatures.csv  # Sample temperature data
```

## Usage

### 1. Creating the Iceberg Table

Run the following command to create the Iceberg table and load the sample data:

```bash
./create_table.py
```

This script will:
- Create a new Iceberg table in the `warehouse` directory
- Load temperature data from `data/temperatures.csv`
- Set up proper metadata handling for DuckDB compatibility

### 2. Querying the Table

You have two options for querying the data:

#### Option 1: Using DuckDB CLI (SQL)

The `query.sql` script is executable and will show the warmest and coldest temperatures:

```bash
./query.sql
```

#### Option 2: Using Python DuckDB API

The `query_table.py` script demonstrates how to query the Iceberg table using DuckDB's Python API:

```bash
./query_table.py
```

## Table Schema

The temperature data includes:
- `airport_code` (string): Three-letter airport code
- `date` (timestamp): Date of temperature reading
- `celsius` (double): Temperature in Celsius

## Implementation Details

The project includes proper handling of Iceberg metadata through `version-hint.text`, which allows DuckDB to locate the correct metadata files. This enables seamless querying of the Iceberg table without needing to specify metadata file locations explicitly.

## Example Queries

The included queries demonstrate:
- Finding the warmest temperatures (sorted by celsius DESC)
- Finding the coldest temperatures (sorted by celsius ASC)
- Formatting dates and temperatures for readable output

## Troubleshooting

If you encounter issues:
1. Ensure all dependencies are installed
2. Check that the `warehouse` directory exists and has proper permissions
3. Verify that DuckDB can find the Iceberg extension
4. If queries fail, ensure `version-hint.text` exists in the metadata directory
