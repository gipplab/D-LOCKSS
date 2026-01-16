#!/bin/bash
# Wrapper script for generate_charts.py using uv
# Supports auto-discovery of CSV files and timestamped output directories

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REQUIREMENTS="$SCRIPT_DIR/requirements.txt"

# Default to auto mode if no arguments provided
if [ $# -eq 0 ]; then
    echo "No arguments provided. Running in auto-discovery mode..."
    echo "Usage: $0 [csv_file] [output_dir] | $0 --auto [--search-dir DIR]"
    echo ""
    AUTO_MODE="--auto"
else
    AUTO_MODE=""
fi

# Check if uv is available
if command -v uv &> /dev/null; then
    if [ -n "$AUTO_MODE" ]; then
        uv run --with-requirements "$REQUIREMENTS" "$SCRIPT_DIR/generate_charts.py" $AUTO_MODE "$@"
    else
        uv run --with-requirements "$REQUIREMENTS" "$SCRIPT_DIR/generate_charts.py" "$@"
    fi
else
    # Fallback to python3 if uv is not available
    if [ -n "$AUTO_MODE" ]; then
        python3 "$SCRIPT_DIR/generate_charts.py" $AUTO_MODE "$@"
    else
        python3 "$SCRIPT_DIR/generate_charts.py" "$@"
    fi
fi
