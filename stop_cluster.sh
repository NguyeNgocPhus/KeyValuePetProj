#!/bin/bash

if [ ! -f .cluster.pids ]; then
    echo "No .cluster.pids file found. Trying to kill any running instances of org.phunn.Main..."
    pkill -f "org.phunn.Main"
    echo "Done."
    exit 0
fi

PIDS=$(cat .cluster.pids)
echo "Stopping cluster processes: $PIDS"

for PID in $PIDS; do
    if ps -p $PID > /dev/null; then
        kill -9 $PID
        echo "Terminated PID $PID"
    else
        echo "PID $PID was already stopped"
    fi
done

rm -f .cluster.pids
echo "Cluster stopped."
