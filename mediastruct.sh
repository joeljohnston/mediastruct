#!/bin/bash

# Check for -m flag
ENABLE_MONITOR=""
while getopts "m" opt; do
    case $opt in
        m)
            ENABLE_MONITOR="-m"
            ;;
        \?)
            echo "Invalid option: -$OPTARG" >&2
            exit 1
            ;;
    esac
done

# Remove the parsed options from the arguments
shift $((OPTIND-1))

# Run each command sequentially with or without the -m flag
echo "Running ingest..."
mediastruct $ENABLE_MONITOR ingest

echo "Running crawl..."
mediastruct $ENABLE_MONITOR crawl

echo "Running dedupe..."
mediastruct $ENABLE_MONITOR dedupe

# Re-run crawl to index files moved to /data/media/duplicates
echo "Running crawl again to re-index duplicates..."
mediastruct $ENABLE_MONITOR crawl

echo "Running archive..."
mediastruct $ENABLE_MONITOR archive

echo "Running validate..."
mediastruct $ENABLE_MONITOR validate

echo "All commands completed."
