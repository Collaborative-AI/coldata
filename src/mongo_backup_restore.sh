#!/bin/bash

# MongoDB Connection Parameters
MONGO_URI="mongodb://localhost:27017"  # Modify with your MongoDB URI
DATABASE_NAME="coldata_tmp"  # Default database name
COLLECTION_NAME="dataset"  # Default collection name (empty means entire database)
BACKUP_DIR="./output/mongodb"  # Default backup directory

# Usage instructions
usage() {
  echo "Usage: $0 {dump|restore} [options]"
  echo "Options for dump:"
  echo "  --db <database_name>    : Specify database name (required for dump)"
  echo "  --collection <collection_name> : Specify collection name (optional)"
  echo "  --out <backup_directory> : Specify output directory for the backup (required)"
  echo ""
  echo "Options for restore:"
  echo "  --db <database_name>    : Specify database name (required for restore)"
  echo "  --collection <collection_name> : Specify collection name (optional)"
  echo "  --from <backup_directory> : Specify backup directory for restore (required)"
  echo ""
  exit 1
}

# Backup the database or collection
dump() {
  if [ -z "$DATABASE_NAME" ] || [ -z "$BACKUP_DIR" ]; then
    echo "Error: Database name and backup directory are required for dump"
    usage
  fi

  if [ -n "$COLLECTION_NAME" ]; then
    echo "Dumping collection '$COLLECTION_NAME' from database '$DATABASE_NAME'..."
    mongodump --uri="$MONGO_URI" --db="$DATABASE_NAME" --collection="$COLLECTION_NAME" --out="$BACKUP_DIR"
  else
    echo "Dumping entire database '$DATABASE_NAME'..."
    mongodump --uri="$MONGO_URI" --db="$DATABASE_NAME" --out="$BACKUP_DIR"
  fi

  echo "Backup completed successfully."
}

# Restore the database or collection
restore() {
  if [ -z "$DATABASE_NAME" ] || [ -z "$BACKUP_DIR" ]; then
    echo "Error: Database name and backup directory are required for restore"
    usage
  fi

  if [ -n "$COLLECTION_NAME" ]; then
    echo "Restoring collection '$COLLECTION_NAME' to database '$DATABASE_NAME'..."
    mongorestore --uri="$MONGO_URI" --db="$DATABASE_NAME" --collection="$COLLECTION_NAME" "$BACKUP_DIR/$DATABASE_NAME/$COLLECTION_NAME.bson"
  else
    echo "Restoring entire database '$DATABASE_NAME'..."
    mongorestore --uri="$MONGO_URI" --db="$DATABASE_NAME" "$BACKUP_DIR/$DATABASE_NAME"
  fi

  echo "Restore completed successfully."
}

# Parse command-line arguments
if [ $# -lt 1 ]; then
  usage
fi

COMMAND=$1
shift

while [ $# -gt 0 ]; do
  case "$1" in
    --db)
      DATABASE_NAME="$2"
      shift 2
      ;;
    --collection)
      COLLECTION_NAME="$2"
      shift 2
      ;;
    --out)
      BACKUP_DIR="$2"
      shift 2
      ;;
    --from)
      BACKUP_DIR="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      usage
      ;;
  esac
done

# Use default values if no parameters are passed
if [ -z "$DATABASE_NAME" ]; then
  DATABASE_NAME="mydatabase"
fi

if [ -z "$BACKUP_DIR" ]; then
  BACKUP_DIR="./backup"
fi

# Execute the appropriate command
case "$COMMAND" in
  dump)
    dump
    ;;
  restore)
    restore
    ;;
  *)
    echo "Invalid command: $COMMAND"
    usage
    ;;
esac
