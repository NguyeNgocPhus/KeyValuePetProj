#!/bin/bash

# Build project if classes don't exist
if [ ! -d "target/classes" ]; then
    echo "Compiling project first..."
    mvn clean compile
fi

# Clean up old WAL logs and stdout logs
echo "Cleaning old WAL and log files..."
rm -f node_*_wal.log node_*.log .cluster.pids

# Start Node 1
echo "Starting Node 1 on port 8001..."
java --enable-preview -cp target/classes org.phunn.Main 1 8001 node_1_wal.log 2:127.0.0.1:8002 3:127.0.0.1:8003 > node_1.log 2>&1 &
PID1=$!

# Start Node 2
echo "Starting Node 2 on port 8002..."
java --enable-preview -cp target/classes org.phunn.Main 2 8002 node_2_wal.log 1:127.0.0.1:8001 3:127.0.0.1:8003 > node_2.log 2>&1 &
PID2=$!

# Start Node 3
echo "Starting Node 3 on port 8003..."
java --enable-preview -cp target/classes org.phunn.Main 3 8003 node_3_wal.log 1:127.0.0.1:8001 2:127.0.0.1:8002 > node_3.log 2>&1 &
PID3=$!

# Save PIDs
echo "$PID1 $PID2 $PID3" > .cluster.pids

echo "Cluster started successfully! Node stdout/stderr redirected to node_1.log, node_2.log, and node_3.log."
echo "PIDs: Node1=$PID1, Node2=$PID2, Node3=$PID3"
