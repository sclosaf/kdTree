#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

OUTPUT_DIR="${PROJECT_ROOT}/data"
OUTPUT_FILE="${OUTPUT_DIR}/dataset.bin"
TXT_OUTPUT="${OUTPUT_DIR}/dataset.txt"

DEFAULT_MIN_VAL="-1e15"
DEFAULT_MAX_VAL="1e15"

NUM_VALUES=""
MIN_VAL="$DEFAULT_MIN_VAL"
MAX_VAL="$DEFAULT_MAX_VAL"

usage()
{
    echo "Usage: $0 -n <num> [-m <min>] [-M <max>]"
    echo "  -n <num>        Number of floats to generate (required)"
    echo "  -m <min>        Minimum value (default: $DEFAULT_MIN_VAL)"
    echo "  -M <max>        Maximum value (default: $DEFAULT_MAX_VAL)"
    exit 1
}

while getopts "n:m:M:" opt; do
    case $opt in
        n) NUM_VALUES="$OPTARG" ;;
        m) MIN_VAL="$OPTARG" ;;
        M) MAX_VAL="$OPTARG" ;;
        *) usage ;;
    esac
done

if [ -z "$NUM_VALUES" ]; then
    echo "Error: -n parameter is required"
    usage
fi

if ! [[ "$NUM_VALUES" =~ ^[0-9]+$ ]] || [ "$NUM_VALUES" -le 0 ]; then
    echo "Error: Number of values must be a positive integer"
    exit 1
fi

if ! echo "$MIN_VAL < $MAX_VAL" | bc -l > /dev/null 2>&1; then
    echo "Error: min must be less than max"
    exit 1
fi

mkdir -p "$OUTPUT_DIR"

rm -f "$OUTPUT_FILE"
rm -f "$TXT_OUTPUT"

echo "Generating $NUM_VALUES float values in range [$MIN_VAL, $MAX_VAL]..."

python3 -c "
    import struct
    import random
    import sys

    num = $NUM_VALUES
    min_val = $MIN_VAL
    max_val = $MAX_VAL

    random.seed()

    with open('$OUTPUT_FILE', 'wb') as f:
        for _ in range(num):
            val = random.uniform(min_val, max_val)
            f.write(struct.pack('f', val))
"

ACTUAL_SIZE=$(stat -c%s "$OUTPUT_FILE" 2>/dev/null)

echo "Binary file created: $OUTPUT_FILE ($ACTUAL_SIZE bytes)"
echo "Extracting first $((NUM_VALUES < 100 ? NUM_VALUES : 100)) points to $TXT_OUTPUT..."

COUNT_TO_READ=$((NUM_VALUES < 100 ? NUM_VALUES : 100))
{
    echo "# First $COUNT_TO_READ float values from dataset.bin"
    echo "# Range: [$MIN_VAL, $MAX_VAL]"
    echo ""

    od -An -tf4 -w4 -v -N $((COUNT_TO_READ * 4)) "$OUTPUT_FILE" | \
    while read -r val; do
        echo "$val" | xargs
    done
} > "$TXT_OUTPUT"
