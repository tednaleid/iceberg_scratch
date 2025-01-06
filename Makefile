.PHONY: all clean create query check-uv check-duckdb

all: clean create query

clean:
	rm -rf warehouse

check-uv:
	@command -v uv >/dev/null 2>&1 || (echo "Error: uv is not installed: brew install uv" && exit 1)

check-duckdb:
	@command -v duckdb >/dev/null 2>&1 || (echo "Error: duckdb is not installed: brew install duckdb" && exit 1)

create: check-uv clean
	./create_table.py

query: check-uv check-duckdb
	@echo "\nRunning SQL query:"
	./query.sql
	@echo "\nRunning Python query:"
	./query_table.py
