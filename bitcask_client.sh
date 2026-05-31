#!/bin/bash

# If API_URL is provided by Docker, use it. Otherwise, default to localhost.
API_URL="${API_URL:-http://localhost:8080}"
CURRENT_TIMESTAMP=$(date +%s)

# Helper function to view all keys
view_all() {
    local thread_suffix=$1
    local filename="${CURRENT_TIMESTAMP}${thread_suffix}.csv"
    
    # Fetch data from Java server and save directly to the CSV file
    # We use curl with -s (silent) to avoid downloading progress bars
    curl -s "${API_URL}/view-all" > "$filename"
    
    # Check if the file has contents, if so, we succeeded
    if [[ -s "$filename" ]]; then
        echo "Data successfully written to $filename"
    else
        echo "Error: Failed to retrieve data or store is empty."
    fi
}

# 1. Handle: ./bitcask_client.sh -view-all
if [[ "$1" == "-view-all" ]]; then
    view_all ""

# 2. Handle: ./bitcask_client.sh -view-key -SOME_KEY
elif [[ "$1" == "-view-key" ]]; then
    KEY="$2"
    if [[ -z "$KEY" ]]; then
        echo "Error: You must provide a key. Usage: ./bitcask_client.sh -view-key <key>"
        exit 1
    fi
    # Fetch specific key and output to stdout
    curl -s "${API_URL}/view-key?id=${KEY}"
    echo "" # Add newline for terminal readability

# 3. Handle: ./bitcask_client.sh -n=100
elif [[ "$1" == -n=* ]]; then
    # Extract the number from the argument (e.g., extract 100 from -n=100)
    NUM_THREADS="${1#*=}"
    
    if ! [[ "$NUM_THREADS" =~ ^[0-9]+$ ]]; then
        echo "Error: Thread count must be a number."
        exit 1
    fi

    echo "Starting $NUM_THREADS concurrent queries..."
    
    for (( i=1; i<=NUM_THREADS; i++ ))
    do
        # The '&' symbol at the end sends the process to the background, 
        # effectively creating concurrent "threads" in bash.
        view_all "_thread_$i" &
    done
    
    # Wait for all background bash processes to finish
    wait
    echo "All $NUM_THREADS threads completed execution."

else
    echo "Usage:"
    echo "  $0 -view-all               (Outputs all keys to timestamp.csv)"
    echo "  $0 -view-key <KEY>         (Prints specific key to stdout)"
    echo "  $0 -n=<NUMBER>             (Runs N threads concurrently generating timestamped files)"
fi