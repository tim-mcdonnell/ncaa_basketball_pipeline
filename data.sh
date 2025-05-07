#!/bin/bash

# Path to your DuckDB database
DB_PATH="data/ncaa_basketball.duckdb"

# Get all tables with their schema in CSV format
echo "Fetching all tables from the database..."
duckdb "$DB_PATH" "COPY (SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema != 'information_schema' ORDER BY table_schema, table_name) TO '/dev/stdout' WITH (FORMAT csv, HEADER);" | tail -n +2 | while IFS=',' read -r SCHEMA TABLE; do
    # Skip empty lines or lines without both schema and table (just in case)
    if [ -z "$SCHEMA" ] || [ -z "$TABLE" ]; then
        continue
    fi

    # Remove potential leading/trailing whitespace or quotes if necessary (depends on exact duckdb csv output)
    SCHEMA=$(echo "$SCHEMA" | xargs)
    TABLE=$(echo "$TABLE" | xargs)

    echo "=========================================================="
    echo "Table: $SCHEMA.$TABLE"
    echo "=========================================================="

    # Run SELECT query for the current table
    duckdb "$DB_PATH" "SELECT * FROM "$SCHEMA"."$TABLE" LIMIT 5;"

    echo ""  # Add an empty line for better readability
done

echo "Finished processing all tables."
