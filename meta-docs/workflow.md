# Implementation Instructions: ESCO Ingestion System Improvements

## Overview
Fix the ESCO data ingestion system to handle automated deployments, prevent race conditions, and ensure idempotent operations. The system uses Docker Compose with Weaviate vector database and needs to handle both cold starts (no data) and warm starts (existing data) gracefully.

## Required Changes

### 1. Environment Detection for Non-Interactive Mode

**File**: `src/esco_ingest.py`

**Task**: Modify the `run_ingest()` method to detect Docker/non-interactive environments and skip user prompts.

```python
# Add at the beginning of run_ingest() method, after checking existing_classes:
if existing_classes and not force_reingest:
    # Check if running in non-interactive mode
    is_docker = os.getenv('DOCKER_ENV') == 'true'
    is_non_interactive = not sys.stdin.isatty() or os.getenv('NON_INTERACTIVE') == 'true'
    
    if is_docker or is_non_interactive:
        logger.info(f"Non-interactive mode detected. Found existing data for classes: {', '.join(existing_classes)}")
        logger.info("Skipping re-ingestion. Use --force-reingest to override.")
        return
    
    # Only show prompt in interactive mode
    if not click.confirm("Do you want to re-ingest these classes?", default=False):
        logger.info("Skipping re-ingestion of existing classes")
        return
```

**File**: `docker-compose.yml`

**Task**: Add environment variable to esco-ingest service:
```yaml
esco-ingest:
  # ... existing config ...
  environment:
    - PYTHONPATH=/app
    - PYTHONUNBUFFERED=1
    - DOCKER_ENV=true  # Add this line
    - NON_INTERACTIVE=true  # Add this line
```

### 2. Ingestion State Management

**File**: `src/esco_weaviate_client.py`

**Task**: Add methods to track ingestion state:

```python
def set_ingestion_metadata(self, status: str, details: dict = None):
    """Store ingestion metadata in Weaviate"""
    try:
        # Create a special metadata object
        metadata = {
            "metaType": "ingestion_status",
            "status": status,  # "in_progress", "completed", "failed"
            "timestamp": datetime.utcnow().isoformat(),
            "version": "1.0",
            "details": json.dumps(details or {})
        }
        
        # Store in a special Metadata class (needs to be added to schema)
        self.client.data_object.create(
            class_name="Metadata",
            data_object=metadata
        )
        logger.info(f"Set ingestion status to: {status}")
    except Exception as e:
        logger.error(f"Failed to set ingestion metadata: {str(e)}")

def get_ingestion_status(self) -> dict:
    """Check if ingestion was completed successfully"""
    try:
        result = (
            self.client.query
            .get("Metadata", ["metaType", "status", "timestamp", "details"])
            .with_where({
                "path": ["metaType"],
                "operator": "Equal",
                "valueString": "ingestion_status"
            })
            .with_sort({"path": ["timestamp"], "order": "desc"})
            .with_limit(1)
            .do()
        )
        
        if result["data"]["Get"]["Metadata"]:
            return result["data"]["Get"]["Metadata"][0]
        return {"status": "not_started"}
    except Exception as e:
        logger.error(f"Failed to get ingestion status: {str(e)}")
        return {"status": "unknown", "error": str(e)}
```

**File**: `resources/schemas/metadata.yaml`

**Task**: Create new schema file:
```yaml
class: Metadata
vectorizer: none
properties:
  - name: metaType
    dataType: [string]
    isIndexed: true
    tokenization: word
  - name: status
    dataType: [string]
    isIndexed: true
    tokenization: word
  - name: timestamp
    dataType: [date]
    isIndexed: true
  - name: version
    dataType: [string]
    isIndexed: false
  - name: details
    dataType: [text]
    isIndexed: false
```

### 3. Idempotent Ingestion Operations

**File**: `src/repositories/weaviate_repository.py`

**Task**: Add upsert functionality:

```python
def upsert(self, data: Dict[str, Any], vector: Optional[List[float]] = None) -> str:
    """Create or update an entity based on conceptUri"""
    try:
        # Check if object exists
        existing = self.get_by_uri(data.get('conceptUri'))
        
        if existing:
            # Update existing object
            object_id = existing.get('_additional', {}).get('id')
            if object_id:
                # Preserve the conceptUri in the update
                update_data = {k: v for k, v in data.items() if k != 'conceptUri'}
                self.client.client.data_object.update(
                    class_name=self.class_name,
                    uuid=object_id,
                    data_object=update_data
                )
                logger.debug(f"Updated existing {self.class_name}: {data.get('conceptUri')}")
                return object_id
        
        # Create new object
        return self.create(data, vector)
    except Exception as e:
        logger.error(f"Failed to upsert {self.class_name}: {str(e)}")
        raise WeaviateError(f"Failed to upsert {self.class_name}: {str(e)}")

def batch_upsert(self, data_list: List[Dict[str, Any]], vectors: List[np.ndarray]) -> List[str]:
    """Batch upsert operation"""
    results = []
    for data, vector in zip(data_list, vectors):
        try:
            result = self.upsert(data, vector)
            results.append(result)
        except Exception as e:
            logger.error(f"Failed to upsert item: {str(e)}")
            results.append(None)
    return results
```

### 4. Modify Ingestion Methods to Use Upsert

**File**: `src/esco_ingest.py`

**Task**: Replace `batch_import` calls with `batch_upsert`:

```python
# In each ingest method (ingest_skills, ingest_occupations, etc.), replace:
# self.skill_repo.batch_import(skills_to_import, skill_vectors)
# With:
self.skill_repo.batch_upsert(skills_to_import, skill_vectors)
```

### 5. Add Ingestion Progress Tracking

**File**: `src/esco_ingest.py`

**Task**: Wrap the `run_ingest` method with state tracking:

```python
def run_ingest(self, force_reingest: bool = False):
    """Run the complete Weaviate ingestion process with state tracking"""
    try:
        # Check current ingestion status
        status = self.client.get_ingestion_status()
        
        if status.get("status") == "completed" and not force_reingest:
            logger.info("Ingestion already completed. Use --force-reingest to re-run.")
            return
        
        if status.get("status") == "in_progress":
            logger.warning("Ingestion already in progress. Checking if stale...")
            # Check if ingestion is stale (>1 hour old)
            timestamp = status.get("timestamp")
            if timestamp:
                ingestion_time = datetime.fromisoformat(timestamp)
                if (datetime.utcnow() - ingestion_time).total_seconds() < 3600:
                    logger.error("Ingestion is currently running. Exiting.")
                    return
            logger.info("Stale ingestion detected, proceeding...")
        
        # Set ingestion as in progress
        self.client.set_ingestion_metadata("in_progress", {"step": "starting"})
        
        # ... existing ingestion code with progress updates ...
        # After each major step:
        self.client.set_ingestion_metadata("in_progress", {"step": "ingesting_skills", "progress": "3/11"})
        
        # At the end:
        self.client.set_ingestion_metadata("completed", {
            "total_skills": skill_count,
            "total_occupations": occupation_count,
            "total_relations": relation_count,
            "completion_time": datetime.utcnow().isoformat()
        })
        
    except Exception as e:
        self.client.set_ingestion_metadata("failed", {"error": str(e), "step": current_step})
        raise
```

### 6. Create Init Container Script

**File**: `scripts/init_ingestion.sh`

**Task**: Create initialization script:

```bash
#!/bin/bash
set -e

echo "Checking ingestion status..."

# Run Python script to check status
python -c "
from src.esco_weaviate_client import WeaviateClient
import sys

try:
    client = WeaviateClient()
    status = client.get_ingestion_status()
    
    if status.get('status') == 'completed':
        print('Ingestion already completed')
        sys.exit(0) # 0: completed, skip
    elif status.get('status') == 'in_progress':
        print('Ingestion in progress, waiting...')
        sys.exit(1) # 1: in_progress, retry
    else: # 'not_started', 'failed', 'unknown'
        print(f'Initial status is \\'{status.get("status")}\\'. Starting new ingestion...')
        sys.exit(2) # 2: needs ingestion
except Exception as e:
    print(f'Error checking status: {e}')
    sys.exit(2) # Default to needing ingestion on error
"

exit_code=$?

if [ $exit_code -eq 0 ]; then
    echo "Data already ingested, skipping..."
    exit 0
elif [ $exit_code -eq 1 ]; then
    echo "Waiting for in-progress ingestion..."
    sleep 30
    exec $0  # Retry
else # exit_code == 2
    echo "Running ingestion..."
    python -m src.esco_cli ingest --config config/weaviate_config.yaml --profile default
    
    echo "Ingestion command finished. Verifying status..."
    python -c "
from src.esco_weaviate_client import WeaviateClient
import sys
import json # Required for details

try:
    client = WeaviateClient()
    final_status_data = client.get_ingestion_status()
    current_final_status = final_status_data.get('status')
    
    if current_final_status == 'completed':
        print('Post-ingestion verification: Status is COMPLETED.')
        sys.exit(0) # Success
    else:
        print(f'Post-ingestion verification: Status is \\'{current_final_status}\\' (expected \\'completed\\').')
        print('Ingestion might have failed to update status correctly or an error occurred during ingestion.')
        # Ensure status is marked as 'failed' to guide the next run
        if current_final_status != 'failed':
            client.set_ingestion_metadata(
                status='failed',
                details={
                    'reason': 'Post-ingestion verification check failed',
                    'final_status_observed': current_final_status,
                    'previous_status_details': final_status_data.get('details', {})
                }
            )
            print(f'Updated status to \\'failed\\'.')
        sys.exit(1) # Failure, non-zero exit due to set -e will fail the script
except Exception as e:
    print(f'Error during post-ingestion status verification: {e}')
    # Attempt to set status to failed if possible
    try:
        client = WeaviateClient()
        client.set_ingestion_metadata(
            status='failed',
            details={'reason': 'Exception during post-ingestion verification', 'error': str(e)}
        )
    except Exception as set_status_e:
        print(f'Additionally failed to set status to \\'failed\\' after verification error: {set_status_e}')
    sys.exit(1) # Failure
    "
fi
```

### 7. Update Docker Compose

**File**: `docker-compose.yml`

**Task**: Replace esco-ingest service with init container pattern:

```yaml
# Remove or comment out the existing esco-ingest service
# Add new init service:

esco-init:
  build: .
  container_name: esco_init
  depends_on:
    weaviate:
      condition: service_healthy
    t2v-transformers:
      condition: service_healthy
  environment:
    - PYTHONPATH=/app
    - PYTHONUNBUFFERED=1
    - DOCKER_ENV=true
    - NON_INTERACTIVE=true
  volumes:
    - ./data:/app/data
    - ./logs:/app/logs
    - ./src:/app/src
    - ./config:/app/config
    - ./scripts:/app/scripts
  command: ["/app/scripts/init_ingestion.sh"]
  restart: "no"
  networks:
    - esco_network

# Update esco-search to depend on esco-init:
esco-search:
  # ... existing config ...
  depends_on:
    esco-init:
      condition: service_completed_successfully
    weaviate:
      condition: service_healthy
    t2v-transformers:
      condition: service_healthy
```

### 8. Add Data Validation Enhancement

**File**: `src/weaviate_semantic_search.py`

**Task**: Enhance `validate_data` to check ingestion status:

```python
def validate_data(self) -> Tuple[bool, Dict[str, Any]]:
    """Validate the data in the Weaviate database including ingestion status"""
    validation_details = {
        "ingestion_status": "unknown",
        "skills_indexed": False,
        # ... existing fields ...
    }
    
    try:
        # Check ingestion status first
        status = self.client.get_ingestion_status()
        validation_details["ingestion_status"] = status.get("status", "unknown")
        
        if status.get("status") != "completed":
            validation_details["errors"].append(f"Ingestion not completed: {status.get('status')}")
            return False, validation_details
        
        # ... rest of existing validation code ...
```

## Testing Instructions

1. **Test Cold Start**:
   ```bash
   docker-compose down -v  # Remove all data
   docker-compose up
   # Verify ingestion runs automatically and completes
   ```

2. **Test Warm Start**:
   ```bash
   docker-compose down  # Keep data
   docker-compose up
   # Verify ingestion is skipped
   ```

3. **Test Failed Ingestion Recovery**:
   ```bash
   # Simulate failure by killing during ingestion
   # Restart and verify it recovers
   ```

4. **Test Concurrent Start**:
   ```bash
   docker-compose up --scale esco-init=2
   # Verify only one ingestion runs
   ```

## Success Criteria

- [ ] No interactive prompts in Docker environment
- [ ] Ingestion runs only once on cold start
- [ ] Ingestion is skipped on warm start
- [ ] Failed ingestions can be recovered
- [ ] No duplicate data on multiple runs
- [ ] Search service only starts after successful ingestion
- [ ] Concurrent ingestion attempts are prevented
- [ ] State is properly tracked and queryable