#!/bin/bash
set -e

# Configuration
MAX_RETRIES=120  # Maximum number of retries (30s * 120 = 1 hour timeout)
HEARTBEAT_INTERVAL=30  # Seconds between heartbeat messages
LOG_PREFIX="[Init Container]"

# Helper function to log messages with timestamp
log() {
    echo "$LOG_PREFIX $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

# Helper function to check if a process is still running
check_process() {
    local pid=$1
    if kill -0 $pid 2>/dev/null; then
        return 0  # Process is running
    else
        return 1  # Process is not running
    fi
}

# Helper function to handle timeouts
handle_timeout() {
    local pid=$1
    local timeout=$2
    local start_time=$(date +%s)
    
    while true; do
        if ! check_process $pid; then
            return 0  # Process completed
        fi
        
        current_time=$(date +%s)
        elapsed=$((current_time - start_time))
        
        if [ $elapsed -ge $timeout ]; then
            kill -TERM $pid 2>/dev/null || true
            log "❌ Process timed out after ${timeout} seconds"
            return 1
        fi
        
        # Send heartbeat message
        if [ $((elapsed % HEARTBEAT_INTERVAL)) -eq 0 ]; then
            log "⏳ Process still running... (${elapsed}s elapsed)"
        fi
        
        sleep 1
    done
}

log "Starting ingestion status check..."

# Run Python script to check status (using service layer)
python -m src.init_ingestion "$@" &
INGESTION_PID=$!

# Monitor the ingestion process with timeout
if ! handle_timeout $INGESTION_PID $((MAX_RETRIES * HEARTBEAT_INTERVAL)); then
    log "❌ Ingestion process timed out after $((MAX_RETRIES * HEARTBEAT_INTERVAL)) seconds"
    exit 4  # New exit code for timeout
fi

# Get the exit code from the Python script
wait $INGESTION_PID
exit_code=$?

case $exit_code in
    0)
        log "✓ Data already ingested or ingestion completed successfully"
        exit 0
        ;;
    1)
        log "⏳ Ingestion in progress, waiting ${HEARTBEAT_INTERVAL} seconds..."
        sleep $HEARTBEAT_INTERVAL
        exec "$0" "$@"  # Retry the current script with same arguments
        ;;
    2)
        log "❌ Ingestion needs to be run but prerequisites failed or manual intervention required"
        log "Check logs for details or run with --force-reingest if needed"
        exit $exit_code
        ;;
    3)
        log "❌ Ingestion or verification failed"
        log "Check logs for detailed error information"
        exit $exit_code
        ;;
    4)
        log "❌ Ingestion process timed out"
        log "The process took longer than $((MAX_RETRIES * HEARTBEAT_INTERVAL)) seconds to complete"
        exit $exit_code
        ;;
    *)
        log "❌ Unexpected exit code $exit_code"
        exit $exit_code
        ;;
esac 