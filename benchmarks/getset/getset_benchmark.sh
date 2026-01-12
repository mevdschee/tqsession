#!/bin/bash
set -e

# Helper to kill process on a port
kill_port() {
    PORT=$1
    if command -v lsof >/dev/null; then
        PIDS=$(lsof -t -i:$PORT || true)
        if [ ! -z "$PIDS" ]; then
            echo "Force killing process(es) on port $PORT: $PIDS"
            kill -9 $PIDS 2>/dev/null || true
        fi
    fi
}

# Cleanup function
cleanup() {
    echo "Stopping servers..."
    if [ ! -z "$TQ_PID" ]; then kill $TQ_PID 2>/dev/null || true; fi
    if [ ! -z "$MEM_PID" ]; then kill $MEM_PID 2>/dev/null || true; fi
    if [ ! -z "$REDIS_PID" ]; then kill $REDIS_PID 2>/dev/null || true; fi
    
    # Force cleanup ports
    kill_port 11221
    kill_port 6380
    kill_port 11222

    rm -rf /tmp/tqsession-bench
    rm -rf /tmp/redis-bench
    rm -f max_rss.tmp results.tmp
    rm -f tqsession-server benchmark-tool
    rm -f redis_bench.log
}
trap cleanup EXIT

# Check dependencies
if ! command -v memcached &> /dev/null; then
    echo "Error: memcached is not installed or not in PATH."
    exit 1
fi
if ! command -v redis-server &> /dev/null; then
    echo "Error: redis-server is not installed or not in PATH."
    exit 1
fi

# Build
echo "Building TQSession and Benchmark Tool..."
go build -o tqsession-server ../../cmd/tqsession
go build -o benchmark-tool .

# Output file
OUTPUT="getset_benchmark.csv"
echo "Mode,Backend,Protocol,Operation,RPS,TimePerReq(ms),MaxMemory(MB)" > $OUTPUT

# Benchmark Configuration
CLIENTS=10
REQUESTS=100000
SIZE=10240
KEYS=100000

# Function to monitor max RSS of a PID
start_monitor() {
    PID=$1
    echo 0 > max_rss.tmp
    (
        while true; do
            if ! kill -0 $PID 2>/dev/null; then break; fi
            rss=$(ps -o rss= -p $PID 2>/dev/null | tr -d ' ' || echo 0)
            if [ -z "$rss" ]; then rss=0; fi
            
            cur_max=$(cat max_rss.tmp)
            if [ "$rss" -gt "$cur_max" ]; then
                echo $rss > max_rss.tmp
            fi
            sleep 0.1
        done
    ) &
    MONITOR_PID=$!
}

stop_monitor() {
    if [ ! -z "$MONITOR_PID" ]; then
        kill $MONITOR_PID 2>/dev/null || true
        wait $MONITOR_PID 2>/dev/null || true
    fi
    MAX_KB=$(cat max_rss.tmp)
    echo $((MAX_KB / 1024))
}

run_benchmark_set() {
    MODE=$1
    SYNC_INTERVAL=$2
    REDIS_FLAGS=$3
    ENABLE_MEMCACHED=$4

    echo "==========================================================="
    echo "Running Benchmark Mode: $MODE"
    echo "==========================================================="

    # Ensure ports are free
    kill_port 11221
    kill_port 6380
    kill_port 11222

    # --- Start TQSession ---
    echo "Starting TQSession (Sync Interval: $SYNC_INTERVAL)..."
    rm -rf /tmp/tqsession-bench
    mkdir -p /tmp/tqsession-bench
    ./tqsession-server -data-dir=/tmp/tqsession-bench -port=:11221 -sync-interval=$SYNC_INTERVAL > /dev/null 2>&1 &
    TQ_PID=$!

    # --- Start Redis ---
    echo "Starting Redis (Flags: $REDIS_FLAGS)..."
    mkdir -p /tmp/redis-bench
    rm -f /tmp/redis-bench/dump.rdb /tmp/redis-bench/appendonly.aof
    redis-server --port 6380 --dir /tmp/redis-bench --daemonize no --protected-mode no $REDIS_FLAGS > /dev/null 2>&1 &
    REDIS_PID=$!

    # --- Start Memcached ---
    MEM_PID=""
    if [ "$ENABLE_MEMCACHED" = "true" ]; then
        echo "Starting Memcached..."
        memcached -p 11222 -m 2048 -u $(whoami) > /dev/null 2>&1 &
        MEM_PID=$!
    fi

    # Wait for startup
    sleep 3

    # --- Run Benchmarks ---

    # TQSession
    echo "Benchmarking TQSession..."
    start_monitor $TQ_PID
    ./benchmark-tool -host localhost:11221 -protocol memcache -label "TQSession" -mode "$MODE" -clients $CLIENTS -requests $REQUESTS -size $SIZE -keys $KEYS -csv > results.tmp
    MEM_MB=$(stop_monitor)
    awk -v mem="$MEM_MB" '{print $0 "," mem}' results.tmp >> $OUTPUT

    # Redis
    echo "Benchmarking Redis..."
    start_monitor $REDIS_PID
    ./benchmark-tool -host localhost:6380 -protocol redis -label "Redis" -mode "$MODE" -clients $CLIENTS -requests $REQUESTS -size $SIZE -keys $KEYS -csv > results.tmp
    MEM_MB=$(stop_monitor)
    awk -v mem="$MEM_MB" '{print $0 "," mem}' results.tmp >> $OUTPUT

    # Memcached
    if [ "$ENABLE_MEMCACHED" = "true" ]; then
        echo "Benchmarking Memcached..."
        start_monitor $MEM_PID
        ./benchmark-tool -host localhost:11222 -protocol memcache -label "Memcached" -mode "$MODE" -clients $CLIENTS -requests $REQUESTS -size $SIZE -keys $KEYS -csv > results.tmp
        MEM_MB=$(stop_monitor)
        awk -v mem="$MEM_MB" '{print $0 "," mem}' results.tmp >> $OUTPUT
    fi

    # Cleanup processes for next round
    kill $TQ_PID 2>/dev/null || true
    kill $REDIS_PID 2>/dev/null || true
    if [ ! -z "$MEM_PID" ]; then kill $MEM_PID 2>/dev/null || true; fi
    wait $TQ_PID 2>/dev/null || true
    wait $REDIS_PID 2>/dev/null || true
    if [ ! -z "$MEM_PID" ]; then wait $MEM_PID 2>/dev/null || true; fi
}

# 1. Mode: Memory (1s sync = default)
run_benchmark_set "Memory" "1s" "--save \"\" --appendonly no" "true"

# 2. Mode: Periodic (1s sync)  
run_benchmark_set "Periodic" "1s" "--appendonly yes --appendfsync everysec" "false"

echo "---------------------------------------------------"
echo "Benchmark completed. Results saved to $OUTPUT"
echo "---------------------------------------------------"
column -s, -t $OUTPUT

echo ""
echo "Done!"
