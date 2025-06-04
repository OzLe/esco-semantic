#!/bin/bash
set -e

echo "Checking ingestion status..."

# Run Python script to check status
python -m src.init_ingestion "$@"

exit_code=$?

if [ $exit_code -eq 0 ]; then
    echo "Data already ingested, skipping..."
    exit 0
elif [ $exit_code -eq 1 ]; then
    echo "Waiting for in-progress ingestion..."
    sleep 30
    exec "$0" "$@"  # Retry the current script with same arguments
else
    echo "Ingestion failed with exit code $exit_code"
    exit $exit_code
fi 