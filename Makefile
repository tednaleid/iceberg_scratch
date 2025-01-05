.PHONY: all clean create query

# Default target runs the full sequence
all: clean create query

# Remove the warehouse directory
clean:
	rm -rf warehouse

# Create and populate the Iceberg table
create: clean
	./create_table.py

# Query the table using both SQL and Python
query:
	@echo "\nRunning SQL query:"
	./query.sql
	@echo "\nRunning Python query:"
	./query_table.py
