#!/bin/bash

PORT=$1

if [ -z "$PORT" ]; then
    echo "Usage: ./start_node.sh <port>"
    echo "Available ports: 8001, 8002, 8003"
    exit 1
fi

# Build project if classes don't exist
if [ ! -d "target/classes" ]; then
    echo "Compiling project..."
    mvn clean compile
fi

if [ "$PORT" = "8001" ]; then
    ID=1
    WAL="node_1_wal.log"
    PEERS="2:127.0.0.1:8002 3:127.0.0.1:8003"
elif [ "$PORT" = "8002" ]; then
    ID=2
    WAL="node_2_wal.log"
    PEERS="1:127.0.0.1:8001 3:127.0.0.1:8003"
elif [ "$PORT" = "8003" ]; then
    ID=3
    WAL="node_3_wal.log"
    PEERS="1:127.0.0.1:8001 2:127.0.0.1:8002"
else
    echo "Error: Invalid port $PORT. Please choose 8001, 8002, or 8003."
    exit 1
fi

echo "============================================="
echo " Starting Node $ID on port $PORT (Foreground)"
echo " Press Ctrl+C to stop this node."
echo "============================================="
echo ""

# Run Java in foreground (no '&')
java --enable-preview -cp target/classes org.phunn.Main $ID $PORT $WAL $PEERS
