#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

OUTPUT_DIR="${PROJECT_ROOT}/data"
OUTPUT_FILE="${OUTPUT_DIR}/dataset.bin"

usage()
{
    echo "Generate a custom random dataset of points."
    echo "Usage: $0 -n <numPoints> -d <dimensions> -m <minValue> -M <maxValue> [-o <outputFile>]"
    echo "  -n: Number of points to generate (int)"
    echo "  -d: Number of dimensions for each point (int)"
    echo "  -m: Minimum value for coordinates (float)"
    echo "  -M: Maximum value for coordinates (float)"
    echo "  -o: Output file path (relative to project root, default: data/dataset.bin)"
    exit 1
}

while getopts "n:d:m:M:o:h" opt; do
    case $opt in
        n) NUM_POINTS=$OPTARG ;;
        d) DIMENSIONS=$OPTARG ;;
        m) MIN_VAL=$OPTARG ;;
        M) MAX_VAL=$OPTARG ;;
        o) OUTPUT_FILE="$PROJECT_ROOT/$OPTARG" ;;
        h) usage ;;
        *) usage ;;
    esac
done

if [ -z "$NUM_POINTS" ] || [ -z "$DIMENSIONS" ] || [ -z "$MIN_VAL" ] || [ -z "$MAX_VAL" ]; then
    echo "Error: Missing required arguments"
    usage
fi

if ! [[ "$NUM_POINTS" =~ ^[0-9]+$ ]] || [ "$NUM_POINTS" -le 0 ]; then
    echo "Error: Number of points must be a positive integer"
    exit 1
fi

if ! [[ "$DIMENSIONS" =~ ^[0-9]+$ ]] || [ "$DIMENSIONS" -le 0 ]; then
    echo "Error: Dimensions must be a positive integer"
    exit 1
fi

if ! [[ "$MIN_VAL" =~ ^-?[0-9]+\.?[0-9]*$ ]] || ! [[ "$MAX_VAL" =~ ^-?[0-9]+\.?[0-9]*$ ]]; then
    echo "Error: Min and max values must be numbers"
    exit 1
fi

if (( $(echo "$MIN_VAL >= $MAX_VAL" | bc -l) )); then
    echo "Error: Min value must be less than max value"
    exit 1
fi

mkdir -p "$(dirname "$OUTPUT_FILE")"

rm -f "$OUTPUT_FILE"
rm -f "${OUTPUT_FILE%.bin}.txt"

echo "Project root: $PROJECT_ROOT"
echo "Generating $NUM_POINTS points with $DIMENSIONS dimensions"
echo "Range: [$MIN_VAL, $MAX_VAL]"
echo "Output file: $OUTPUT_FILE"
echo ""

TOTAL_FLOATS=$((NUM_POINTS * DIMENSIONS))
RANGE=$(echo "$MAX_VAL - $MIN_VAL" | bc -l)

{
    printf "%04x%04x" "$NUM_POINTS" "$DIMENSIONS" | xxd -r -p

    dd if=/dev/urandom bs=4 count=$TOTAL_FLOATS 2>/dev/null | \
    od -An -tu4 -w4 -v | \
    perl -ne '
        use bytes;
        BEGIN { binmode STDOUT; }
        chomp;
        # Converti il numero letto in float tra 0 e 1
        $rand = $_ / 4294967296;
        # Scala al range desiderato
        $val = '$MIN_VAL' + $rand * '$RANGE';
        # Pack come float little-endian (4 byte)
        print pack("f", $val);
    '
} > "$OUTPUT_FILE"

TEXT_OUTPUT="${OUTPUT_FILE%.bin}.txt"
{
    echo "# Format: each line contains $DIMENSIONS coordinates separated by spaces"
    echo "# Points: $NUM_POINTS, Dimensions: $DIMENSIONS, Range: [$MIN_VAL, $MAX_VAL]"
    echo "# Showing first 100 points only"

    TEXT_FLOATS=$((100 * DIMENSIONS))
    if [ $TEXT_FLOATS -gt $TOTAL_FLOATS ]; then
        TEXT_FLOATS=$TOTAL_FLOATS
    fi

    dd if=/dev/urandom bs=4 count=$TEXT_FLOATS 2>/dev/null | \
    od -An -tu4 -w4 -v | \
    perl -ne '
        chomp;
        BEGIN { $count = 0; $line = ""; $d = '$DIMENSIONS'; }
        $rand = $_ / 4294967296;
        $val = '$MIN_VAL' + $rand * '$RANGE';
        $line .= sprintf("%.6f", $val);
        $count++;
        if ($count == $d) {
            print "$line\n";
            $line = "";
            $count = 0;
        } else {
            $line .= " ";
        }
    '
} > "$TEXT_OUTPUT"

BIN_SIZE=$(stat -c%s "$OUTPUT_FILE" 2>/dev/null)
TXT_SIZE=$(stat -c%s "$TEXT_OUTPUT" 2>/dev/null)
EXPECTED_SIZE=$((8 + (NUM_POINTS * DIMENSIONS * 4)))

echo ""
echo "Generation completed in ${SECONDS} seconds"
echo "Binary file: $OUTPUT_FILE ($BIN_SIZE bytes) - ALL $NUM_POINTS points"
echo "Text file: $TEXT_OUTPUT ($TXT_SIZE bytes) - first 100 points only"
echo ""
echo "Binary format:"
echo "  - Header: 8 bytes (4 bytes for num_points, 4 bytes for dimensions)"
echo "  - Data: $((NUM_POINTS * DIMENSIONS * 4)) bytes of float values (IEEE 754, 4 bytes each)"
echo ""
