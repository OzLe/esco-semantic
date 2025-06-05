#!/bin/bash
set -e

# Configuration
MAX_RETRIES=120  # Maximum number of retries (30s * 120 = 1 hour timeout)
HEARTBEAT_INTERVAL=30  # Seconds between heartbeat messages
HEARTBEAT_LOG_INTERVAL=300  # Log heartbeat every 5 minutes (reduced frequency)
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

# Helper function to handle timeouts with reduced heartbeat spam
handle_timeout() {
    local pid=$1
    local timeout=$2
    local start_time=$(date +%s)
    local last_heartbeat_log=0
    
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
        
        # Send heartbeat message only at configured intervals to reduce spam
        if [ $((elapsed % HEARTBEAT_LOG_INTERVAL)) -eq 0 ] && [ $elapsed -gt $last_heartbeat_log ]; then
            log "⏳ Initialization in progress (${elapsed}s elapsed, timeout: ${timeout}s)"
            last_heartbeat_log=$elapsed
        fi
        
        sleep $HEARTBEAT_INTERVAL
    done
}

log "Starting ESCO ingestion initialization"

# Run Python script to check status (using service layer)
python -m src.init_ingestion "$@" &
INGESTION_PID=$!

# Monitor the ingestion process with timeout
if ! handle_timeout $INGESTION_PID $((MAX_RETRIES * HEARTBEAT_INTERVAL)); then
    log "❌ Initialization timed out after $((MAX_RETRIES * HEARTBEAT_INTERVAL)) seconds"
    exit 4  # New exit code for timeout
fi

# Get the exit code from the Python script
wait $INGESTION_PID
exit_code=$?

case $exit_code in
    0)
        log "✓ Initialization completed successfully"
        exit 0
        ;;
    1)
        log "⏳ Ingestion in progress, will retry in ${HEARTBEAT_INTERVAL}s"
        sleep $HEARTBEAT_INTERVAL
        exec "$0" "$@"  # Retry the current script with same arguments
        ;;
    2)
        log "❌ Manual intervention required"
        log "Check logs or run with --force-reingest if needed"
        exit $exit_code
        ;;
    3)
        log "❌ Initialization failed"
        log "Check logs for detailed error information"
        exit $exit_code
        ;;
    4)
        log "❌ Process timeout"
        log "Initialization took longer than $((MAX_RETRIES * HEARTBEAT_INTERVAL)) seconds"
        exit $exit_code
        ;;
    *)
        log "❌ Unexpected exit code $exit_code"
        exit $exit_code
        ;;
esac 