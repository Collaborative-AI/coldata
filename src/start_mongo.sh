#!/bin/bash
# Path to your MongoDB configuration file
# MONGO_CONFIG="/path/to/your/mongod.cfg"
MONGO_CONFIG="E:\MongoDB\Server\7.0\bin\mongod.cfg"

# Start MongoDB with the specified config file
echo "Start MongoDB with config $MONGO_CONFIG"
mongod --config "$MONGO_CONFIG"
