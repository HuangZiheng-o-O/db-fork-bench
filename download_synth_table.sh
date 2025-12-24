#!/bin/bash
# Download TPC-C schema SQL file from Google Drive

FILE_ID="17ivpB7fB4nsQY-6eYG136jiMoyAxJxar"
OUTPUT_DIR="db_setup"
OUTPUT_FILE="$OUTPUT_DIR/microbench.sql"

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Download using gdown (handles Google Drive large files better than curl)
# Install gdown if needed: pip install gdown
echo "Downloading SQL file from Google Drive..."

if command -v gdown &> /dev/null; then
    gdown "https://drive.google.com/uc?id=$FILE_ID" -O "$OUTPUT_FILE"
else
    echo "gdown not found. Installing..."
    pip install gdown
    gdown "https://drive.google.com/uc?id=$FILE_ID" -O "$OUTPUT_FILE"
fi

if [ -f "$OUTPUT_FILE" ]; then
    echo "Downloaded successfully to $OUTPUT_FILE"
    echo "File size: $(ls -lh "$OUTPUT_FILE" | awk '{print $5}')"
else
    echo "Download failed"
    exit 1
fi
