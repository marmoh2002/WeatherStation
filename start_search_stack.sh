#!/bin/bash

# Determine the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Navigate to the script's directory so docker compose can find docker-compose.yml
cd "$SCRIPT_DIR" || exit 1

echo "Starting Parquet Indexer, Kibana, and Elasticsearch services..."
docker compose up parquet-indexer kibana elastic-search -d

# Check if the command succeeded
if [ $? -eq 0 ]; then
    echo "Waiting for Kibana to fully start (this may take a minute)..."
    until curl -s -f http://localhost:5601/api/status > /dev/null; do
        printf "."
        sleep 2
    done
    echo ""
    echo "Services started successfully! Open http://localhost:5601"
else
    echo "Failed to start services."
    exit 1
fi
