#!/bin/bash

if [ "$1" == "sentence-transformers" ]; then
    export TRANSFORMER_IMAGE="semitechnologies/transformers-inference:sentence-transformers-all-MiniLM-L6-v2"
    echo "Switching to sentence-transformers (all-MiniLM-L6-v2)"
elif [ "$1" == "contextuary" ]; then
    export TRANSFORMER_IMAGE="semitechnologies/transformers-inference:contextuary"
    echo "Switching to contextuary"
else
    echo "Usage: ./switch_vectorizer.sh [sentence-transformers|contextuary]"
    exit 1
fi

# Stop and remove existing containers
docker-compose down

# Start with new configuration
docker-compose up -d 