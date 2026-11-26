#!/bin/bash

# If API_URL is provided by Docker, use it. Otherwise, default to localhost.
API_URL="${API_URL:-http://localhost:8080}"
API_URL="${API_URL%/}"
CURRENT_TIMESTAMP=$(date +%s)
_BENCH_TMP_RESULTS=""

cleanup_bench() {
    if [[ -n "$_BENCH_TMP_RESULTS" && -f "$_BENCH_TMP_RESULTS" ]]; then
        rm -f "$_BENCH_TMP_RESULTS"
    fi
}

print_usage() {
    echo "Usage:"
    echo "  $0 -view-all                                   (Outputs all keys to timestamp.csv)"
    echo "  $0 -view-key <KEY>                             (Prints specific key to stdout)"
    echo "  $0 -n=<NUMBER>                                 (Runs N threads concurrently generating timestamped CSV files)"
    echo "  $0 -bench-key [-c=<CONCURRENCY>] [-n=<REQUESTS>] [-keys=<RANGE|CSV>]"
    echo "                                                 (Load tests /view-key?id=<random_station_id> without CSV writes)"
    echo ""
    echo "Benchmark options (-bench-key):"
    echo "  -c=<N>, --concurrency=<N>   Concurrent worker connections (default: 10)"
    echo "  -n=<N>, --requests=<N>      Total HTTP requests to execute (default: 1000)"
    echo "  -keys=<SPEC>, --keys=<SPEC> Station ID pool: range (e.g. 1-10) or comma list (e.g. 1,2,101)"
    echo "                              Default: auto-discovered from /view-all, falling back to 1-10"
}

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

# Benchmark /view-key without writing per-request CSV files to disk
bench_key() {
    local concurrency=10
    local total_requests=1000
    local keys_spec=""

    while [[ $# -gt 0 ]]; do
        case "$1" in
            -bench-key|--bench-key)
                shift
                ;;
            -c=*|--concurrency=*)
                concurrency="${1#*=}"
                shift
                ;;
            -c|--concurrency)
                if [[ $# -lt 2 ]]; then
                    echo "Error: $1 requires a positive integer." >&2
                    exit 1
                fi
                concurrency="$2"
                shift 2
                ;;
            -n=*|--requests=*)
                total_requests="${1#*=}"
                shift
                ;;
            -n|--requests)
                if [[ $# -lt 2 ]]; then
                    echo "Error: $1 requires a positive integer." >&2
                    exit 1
                fi
                total_requests="$2"
                shift 2
                ;;
            -keys=*|--keys=*)
                keys_spec="${1#*=}"
                shift
                ;;
            -keys|--keys)
                if [[ $# -lt 2 ]]; then
                    echo "Error: $1 requires a key range or comma-separated list." >&2
                    exit 1
                fi
                keys_spec="$2"
                shift 2
                ;;
            -h|--help)
                print_usage
                exit 0
                ;;
            *)
                echo "Error: Unknown option '$1' for -bench-key." >&2
                print_usage >&2
                exit 1
                ;;
        esac
    done

    if ! [[ "$concurrency" =~ ^[1-9][0-9]*$ ]]; then
        echo "Error: Concurrency (-c) must be a positive integer." >&2
        exit 1
    fi

    if ! [[ "$total_requests" =~ ^[1-9][0-9]*$ ]]; then
        echo "Error: Request count (-n) must be a positive integer." >&2
        exit 1
    fi

    local keys_mode=""
    local keys_csv=""
    local min_id=1
    local max_id=10
    local key_source=""

    if [[ -n "$keys_spec" ]]; then
        if [[ "$keys_spec" =~ ^([0-9]+)-([0-9]+)$ ]]; then
            min_id="${BASH_REMATCH[1]}"
            max_id="${BASH_REMATCH[2]}"
            if (( min_id > max_id )); then
                echo "Error: Invalid key range '$keys_spec' (start > end)." >&2
                exit 1
            fi
            keys_mode="range"
            key_source="range ${min_id}..${max_id} (-keys)"
        elif [[ "$keys_spec" =~ ^[0-9]+(,[0-9]+)*$ ]]; then
            keys_mode="list"
            keys_csv="$keys_spec"
            local explicit_count
            explicit_count=$(LC_ALL=C awk -F',' '{print NF}' <<< "$keys_csv")
            key_source="${explicit_count} key(s) (-keys)"
        else
            echo "Error: -keys must be a range (e.g. 1-10) or comma-separated integers (e.g. 1,2,101)." >&2
            exit 1
        fi
    else
        local discovered_csv
        discovered_csv=$(curl -fsS --connect-timeout 3 --max-time 5 "${API_URL}/view-all" 2>/dev/null \
            | LC_ALL=C awk -F',' '/^[0-9]+,/ && !seen[$1]++ { if (out != "") out = out ","; out = out $1; if (++cnt >= 3000) exit } END { print out }')
        if [[ -n "$discovered_csv" ]]; then
            keys_mode="list"
            keys_csv="$discovered_csv"
            local discovered_count
            discovered_count=$(LC_ALL=C awk -F',' '{print NF}' <<< "$keys_csv")
            key_source="${discovered_count} live key(s) from /view-all"
        else
            keys_mode="range"
            min_id=1
            max_id=10
            key_source="default range 1..10 (fallback; /view-all returned no keys)"
        fi
    fi

    _BENCH_TMP_RESULTS=$(mktemp) || {
        echo "Error: Failed to create temporary metrics file." >&2
        exit 1
    }
    trap cleanup_bench EXIT INT TERM

    local workers=$concurrency
    if (( workers > total_requests )); then
        workers=$total_requests
    fi
    local base_reqs=$((total_requests / workers))
    local extra_reqs=$((total_requests % workers))

    echo "Running /view-key benchmark: requests=${total_requests}, concurrency=${workers} (requested=${concurrency}), keys=${key_source}"

    local start_ts end_ts
    if [[ -n "${EPOCHREALTIME:-}" ]]; then
        start_ts="$EPOCHREALTIME"
    else
        start_ts=$(date +%s)
    fi

    local w worker_reqs seed
    for (( w = 1; w <= workers; w++ )); do
        worker_reqs=$base_reqs
        if (( w <= extra_reqs )); then
            ((worker_reqs++))
        fi
        seed=$(( (RANDOM * 32768 + RANDOM + w * 7919) % 2147483647 + 1 ))
        (
            LC_ALL=C awk -v n="$worker_reqs" \
                -v mode="$keys_mode" \
                -v keys_csv="$keys_csv" \
                -v min_id="$min_id" \
                -v max_id="$max_id" \
                -v base_url="${API_URL}/view-key?id=" \
                -v seed="$seed" '
            BEGIN {
                srand(seed)
                if (mode == "list") {
                    k = split(keys_csv, arr, ",")
                } else {
                    span = max_id - min_id + 1
                }
                for (i = 1; i <= n; i++) {
                    if (i > 1) print "next"
                    if (mode == "list") {
                        id = arr[int(rand() * k) + 1]
                    } else {
                        id = min_id + int(rand() * span)
                    }
                    printf "url = \"%s%s\"\noutput = \"/dev/null\"\nsilent\nconnect-timeout = 5\nmax-time = 10\nwrite-out = \"%%{time_total} %%{http_code}\\n\"\n", base_url, id
                }
            }' | curl -K - | LC_ALL=C awk '{ print $0; fflush() }' >> "$_BENCH_TMP_RESULTS"
        ) &
    done
    wait

    if [[ -n "${EPOCHREALTIME:-}" ]]; then
        end_ts="$EPOCHREALTIME"
    else
        end_ts=$(date +%s)
    fi

    local elapsed_sec
    elapsed_sec=$(LC_ALL=C awk -v s="$start_ts" -v e="$end_ts" 'BEGIN { d = e - s; if (d <= 0) d = 0.000001; printf "%.6f", d }')

    LC_ALL=C sort -n "$_BENCH_TMP_RESULTS" | LC_ALL=C awk \
        -v elapsed="$elapsed_sec" \
        -v target="${API_URL}/view-key?id=<random_station_id>" \
        -v concurrency="$workers" \
        -v key_source="$key_source" '
    function pct(p, n,    idx) {
        if (n <= 0) return 0
        idx = int((p * n + 99) / 100)
        if (idx < 1) idx = 1
        if (idx > n) idx = n
        return lat[idx]
    }
    /^[0-9]+(\.[0-9]+)? [0-9][0-9][0-9]$/ {
        n++
        lat[n] = $1 * 1000.0
        code = $2
        if (code ~ /^2[0-9][0-9]$/) {
            c2xx++
        } else if (code == "404") {
            c404++
        } else if (code ~ /^5[0-9][0-9]$/) {
            c5xx++
        } else {
            cother++
        }
    }
    END {
        rps = (elapsed > 0) ? (n / elapsed) : 0
        printf "\n--- /view-key Benchmark Summary ---\n"
        printf "Target URL:       %s\n", target
        printf "Concurrency:      %d\n", concurrency
        printf "Key source:       %s\n", key_source
        printf "Duration (s):     %.3f\n", elapsed
        printf "Total requests:   %d\n", n + 0
        printf "HTTP 2xx:         %d\n", c2xx + 0
        printf "HTTP 404:         %d\n", c404 + 0
        printf "HTTP 5xx:         %d\n", c5xx + 0
        printf "HTTP other/err:   %d\n", cother + 0
        printf "Requests/sec:     %.2f\n", rps
        printf "Latency P50 (ms): %.2f\n", pct(50, n)
        printf "Latency P95 (ms): %.2f\n", pct(95, n)
        printf "Latency P99 (ms): %.2f\n", pct(99, n)
    }'
}

for arg in "$@"; do
    if [[ "$arg" == "-bench-key" || "$arg" == "--bench-key" ]]; then
        bench_key "$@"
        exit $?
    fi
done

# 1. Handle: ./bitcask_client.sh -view-all
if [[ "$1" == "-view-all" ]]; then
    view_all ""

# 2. Handle: ./bitcask_client.sh -view-key <KEY>
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
    print_usage
fi
