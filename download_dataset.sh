OUTPUT_DIR="testgraphs"
PYTHON_SCRIPT="remap_graph.py"

download_file() {
    NAME=$1
    URL=$2
    echo "Downloading dataset from $URL..." >&2  
    curl -L "$URL" -o "${OUTPUT_DIR}/${NAME}.gz" >&2
    echo "Extracting file..." >&2
    gunzip -f "${OUTPUT_DIR}/${NAME}.gz" >&2
    echo "${OUTPUT_DIR}/${NAME}"  
}

download_and_remap() {
    GRAPH_NAME=$1
    GRAPH_URL=$2
    TOP5000=$3  # optional

    mkdir -p "$OUTPUT_DIR"

    GRAPH_FILE=$(download_file "${GRAPH_NAME}.txt" "$GRAPH_URL")

    if [ -n "$TOP5000" ]; then
        TOP5000_FILE=$(download_file "${GRAPH_NAME}_top5000.txt" "$TOP5000")
        echo "Remapping with community file..."
        python3 "$PYTHON_SCRIPT" "$GRAPH_FILE" "$TOP5000_FILE"
    else
        echo "Remapping without community file..."
        python3 "$PYTHON_SCRIPT" "$GRAPH_FILE"
    fi
}

# Amazon (with top5000)
download_and_remap \
    "amazon" \
    "https://snap.stanford.edu/data/bigdata/communities/com-amazon.ungraph.txt.gz" \
    "https://snap.stanford.edu/data/bigdata/communities/com-amazon.top5000.cmty.txt.gz"

# DBLP (with top5000)
download_and_remap \
    "dblp" \
    "https://snap.stanford.edu/data/bigdata/communities/com-dblp.ungraph.txt.gz" \
    "https://snap.stanford.edu/data/bigdata/communities/com-dblp.top5000.cmty.txt.gz"

# YouTube
download_and_remap \
    "youtube" \
    "https://snap.stanford.edu/data/bigdata/communities/com-youtube.ungraph.txt.gz"
