#!/bin/bash
set -e

echo "Checking ingestion status..."

# Run Python script to check status (using service layer)
python -m src.init_ingestion "$@"

exit_code=$?

if [ $exit_code -eq 0 ]; then
    echo "✓ Data already ingested or ingestion completed successfully"
    exit 0
elif [ $exit_code -eq 1 ]; then
    echo "⏳ Ingestion in progress, waiting 30 seconds..."
    sleep 30
    exec "$0" "$@"  # Retry the current script with same arguments
elif [ $exit_code -eq 2 ]; then
    echo "❌ Ingestion needs to be run but prerequisites failed or manual intervention required"
    echo "Check logs for details or run with --force-reingest if needed"
    exit $exit_code
elif [ $exit_code -eq 3 ]; then
    echo "❌ Ingestion or verification failed"
    echo "Check logs for detailed error information"
    exit $exit_code
else
    echo "❌ Unexpected exit code $exit_code"
    exit $exit_code
fi 