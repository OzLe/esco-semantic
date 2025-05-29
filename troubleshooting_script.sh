#!/bin/bash

# ESCO Schema Troubleshooting Script
# This script helps resolve common Weaviate schema issues

set -e

echo "🔧 ESCO Schema Troubleshooting Script"
echo "====================================="

# Function to check if Docker services are running
check_services() {
    echo "🔍 Checking Docker services..."
    
    if ! docker compose ps | grep -q "Up"; then
        echo "❌ Docker services are not running"
        echo "💡 Starting services..."
        docker compose up -d weaviate t2v-transformers
        
        echo "⏳ Waiting for services to start..."
        sleep 30
    else
        echo "✅ Docker services are running"
    fi
}

# Function to check Weaviate health
check_weaviate_health() {
    echo "🏥 Checking Weaviate health..."
    
    max_attempts=30
    attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        if curl -s http://localhost:8080/v1/.well-known/ready > /dev/null; then
            echo "✅ Weaviate is healthy"
            return 0
        fi
        
        echo "⏳ Waiting for Weaviate... (attempt $attempt/$max_attempts)"
        sleep 2
        ((attempt++))
    done
    
    echo "❌ Weaviate is not responding after $max_attempts attempts"
    return 1
}

# Function to reset Weaviate data
reset_weaviate_data() {
    echo "🗑️  Resetting Weaviate data..."
    
    # Stop services
    docker compose down
    
    # Remove Weaviate data volume
    docker volume rm esco-ingest_weaviate_data 2>/dev/null || true
    
    # Start services again
    docker compose up -d weaviate t2v-transformers
    
    echo "⏳ Waiting for services to restart..."
    sleep 30
}

# Function to check schema status
check_schema_status() {
    echo "📊 Checking schema status..."
    
    python -c "
from src.esco_weaviate_client import WeaviateClient
try:
    client = WeaviateClient()
    schema = client.client.schema.get()
    classes = schema.get('classes', [])
    print(f'Found {len(classes)} classes in schema')
    for cls in classes:
        print(f'  - {cls.get(\"class\", \"Unknown\")}')
except Exception as e:
    print(f'Error checking schema: {str(e)}')
"
}

# Function to reset schema only (keep data)
reset_schema_only() {
    echo "🔄 Resetting schema only..."
    
    python -c "
from src.esco_weaviate_client import WeaviateClient
try:
    client = WeaviateClient()
    client.reset_schema()
    print('✅ Schema reset completed successfully')
except Exception as e:
    print(f'❌ Error resetting schema: {str(e)}')
    exit(1)
"
}

# Main menu
show_menu() {
    echo ""
    echo "🛠️  Troubleshooting Options:"
    echo "1. Check services and health"
    echo "2. Check schema status"
    echo "3. Reset schema only (keep data)"
    echo "4. Reset all data (nuclear option)"
    echo "5. Run full ingestion after reset"
    echo "6. Exit"
    echo ""
}

# Option handlers
handle_option_1() {
    check_services
    check_weaviate_health
}

handle_option_2() {
    check_services
    if check_weaviate_health; then
        check_schema_status
    fi
}

handle_option_3() {
    check_services
    if check_weaviate_health; then
        reset_schema_only
        echo "✅ Schema reset complete. You can now run ingestion."
    fi
}

handle_option_4() {
    echo "⚠️  WARNING: This will delete ALL data!"
    read -p "Are you sure? (type 'yes' to confirm): " confirm
    if [ "$confirm" = "yes" ]; then
        reset_weaviate_data
        if check_weaviate_health; then
            echo "✅ Data reset complete. You can now run ingestion."
        fi
    else
        echo "❌ Reset cancelled"
    fi
}

handle_option_5() {
    echo "🚀 Running full ingestion..."
    
    # Ensure services are up
    check_services
    if ! check_weaviate_health; then
        echo "❌ Cannot run ingestion - Weaviate is not healthy"
        return 1
    fi
    
    # Run ingestion
    echo "📥 Starting ESCO data ingestion..."
    python src/esco_cli.py ingest --config config/weaviate_config.yaml --delete-all
}

# Main script loop
main() {
    while true; do
        show_menu
        read -p "Choose an option (1-6): " choice
        
        case $choice in
            1) handle_option_1 ;;
            2) handle_option_2 ;;
            3) handle_option_3 ;;
            4) handle_option_4 ;;
            5) handle_option_5 ;;
            6) 
                echo "👋 Goodbye!"
                exit 0
                ;;
            *)
                echo "❌ Invalid option. Please choose 1-6."
                ;;
        esac
        
        echo ""
        read -p "Press Enter to continue..."
    done
}

# Check if we're in the right directory
if [ ! -f "docker-compose.yml" ]; then
    echo "❌ Error: docker-compose.yml not found"
    echo "💡 Please run this script from the project root directory"
    exit 1
fi

# Check if Python environment is set up
if ! python -c "import src.esco_weaviate_client" 2>/dev/null; then
    echo "❌ Error: Python environment not set up correctly"
    echo "💡 Please ensure you have installed the required dependencies"
    echo "   Run: pip install -r requirements.txt"
    exit 1
fi

# Run main menu
main