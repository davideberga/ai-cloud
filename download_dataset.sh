#!/bin/bash

OUTPUT_DIR="testgraphs"
PYTHON_SCRIPT="remap_graph.py"

download_and_remap() {
    DATASET_NAME=$1
    DATASET_URL=$2
    echo "Downloading dataset from $DATASET_URL..."
    curl -L "$DATASET_URL" -o "${OUTPUT_DIR}/${DATASET_NAME}.txt.gz"
    echo "Extracting file..."
    gunzip -f "${OUTPUT_DIR}/${DATASET_NAME}.txt.gz"
    FILE_PATH="${OUTPUT_DIR}/${DATASET_NAME}.txt"
    echo "Dataset saved in: $FILE_PATH"
    
    echo "Remapping..."
    python3 "$PYTHON_SCRIPT" "$FILE_PATH"
}

mkdir -p "$OUTPUT_DIR"

download_and_remap "amazon" "https://snap.stanford.edu/data/bigdata/communities/com-amazon.ungraph.txt.gz"
download_and_remap "dblp" "https://snap.stanford.edu/data/bigdata/communities/com-dblp.ungraph.txt.gz"
download_and_remap "youtube" "https://snap.stanford.edu/data/bigdata/communities/com-youtube.ungraph.txt.gz"