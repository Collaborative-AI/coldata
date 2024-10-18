#!/bin/bash

# NOTE: On Windows, make sure Docker Desktop is open before running Docker commands

# Get the action (start or stop) from the first argument
ACTION=${1:-start}
MILVUS_CONTAINER="milvus-standalone"

if [ "$ACTION" == "start" ]; then
    echo "Starting Milvus container: $MILVUS_CONTAINER"
    docker start $MILVUS_CONTAINER
    echo "Milvus container has been started."

elif [ "$ACTION" == "stop" ]; then
    echo "Stopping Milvus container: $MILVUS_CONTAINER"
    docker stop $MILVUS_CONTAINER
    echo "Milvus container has been stopped."

else
    echo "Invalid command. Use './manage_milvus.sh start' or './manage_milvus.sh stop'"
fi
