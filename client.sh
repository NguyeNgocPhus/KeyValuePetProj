#!/bin/bash

PORT=$1
CMD=$2
KEY=$3
VAL=$4

if [ -z "$PORT" ] || [ -z "$CMD" ] || [ -z "$KEY" ]; then
    echo "Usage: ./client.sh <port> <GET|SET|GET_LOCAL> <key> [value]"
    echo ""
    echo "Examples:"
    echo "  ./client.sh 8001 SET username alice"
    echo "  ./client.sh 8001 GET username"
    echo "  ./client.sh 8002 GET_LOCAL username"
    exit 1
fi

if [ "$CMD" = "SET" ]; then
    if [ -z "$VAL" ]; then
        echo "Error: SET command requires a value"
        exit 1
    fi
    JSON="{\"type\":\"SET\",\"key\":\"$KEY\",\"value\":\"$VAL\"}"
elif [ "$CMD" = "GET" ]; then
    JSON="{\"type\":\"GET\",\"key\":\"$KEY\"}"
elif [ "$CMD" = "GET_LOCAL" ]; then
    JSON="{\"type\":\"GET_LOCAL\",\"key\":\"$KEY\"}"
else
    echo "Error: Unknown command $CMD. Must be SET, GET, or GET_LOCAL."
    exit 1
fi

echo "Sending to port $PORT: $JSON"
echo "$JSON" | nc localhost $PORT
echo ""
