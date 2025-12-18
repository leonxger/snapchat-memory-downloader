#!/bin/bash
#
# step1_html_to_csv.sh - Snapchat Memories HTML to CSV Converter (Linux/macOS Version)
#
# OPTIMIZED VERSION - Uses awk for 10-100x faster parsing
# Converts a Snapchat Memories HTML export into CSV format.
# Parses HTML tables to extract date, media type, lat/lon, and download URLs.
#
# Usage:
#   ./step1_html_to_csv.sh --input <path> --output <path>
#

# ============================================================================
# COLORS AND VISUAL CONSTANTS
# ============================================================================
readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[1;33m'
readonly CYAN='\033[0;36m'
readonly GRAY='\033[0;90m'
readonly WHITE='\033[1;37m'
readonly BOLD='\033[1m'
readonly NC='\033[0m'

# Progress bar characters
readonly PROG_FILL='━'
readonly PROG_EMPTY='─'
readonly PROG_WIDTH=40

# ============================================================================
# DEFAULT VALUES
# ============================================================================
INPUT_FILE=""
OUTPUT_FILE=""

# ============================================================================
# BANNER
# ============================================================================
print_banner() {
    echo -e "${CYAN}"
    echo "╔═══════════════════════════════════════════════════════════════╗"
    echo "║                                                               ║"
    echo "║      ██╗  ██╗████████╗███╗   ███╗██╗     ██████╗ ██████╗███╗  ║"
    echo "║      ██║  ██║╚══██╔══╝████╗ ████║██║    ╚════██╗██╔════╝██║   ║"
    echo "║      ███████║   ██║   ██╔████╔██║██║     █████╔╝██║     ██║   ║"
    echo "║      ██╔══██║   ██║   ██║╚██╔╝██║██║    ██╔═══╝ ██║     ╚═╝   ║"
    echo "║      ██║  ██║   ██║   ██║ ╚═╝ ██║██████╗███████╗╚█████╗ ██╗   ║"
    echo "║      ╚═╝  ╚═╝   ╚═╝   ╚═╝     ╚═╝╚═════╝╚══════╝ ╚════╝ ╚═╝   ║"
    echo "║                                                               ║"
    echo "║              HTML to CSV Converter  v2.0                      ║"
    echo "║           Snapchat Memories Parser (Optimized)                ║"
    echo "║                                                               ║"
    echo "╚═══════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

# ============================================================================
# HELP
# ============================================================================
print_help() {
    echo "html_to_csv.sh - Snapchat Memories HTML to CSV Converter"
    echo ""
    echo "USAGE:"
    echo "    ./step1_html_to_csv.sh --input <path> --output <path>"
    echo ""
    echo "ARGUMENTS:"
    echo "    --input <path>      Path to Snapchat HTML export file (required)"
    echo "    --output <path>     Path to output CSV file (required)"
    echo "    --help              Show this help message"
    echo ""
    echo "EXAMPLE:"
    echo "    ./step1_html_to_csv.sh --input memories_history.html --output snap_data.csv"
}

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

# Portable realpath fallback for macOS
get_realpath() {
    local path="$1"
    if command -v realpath &>/dev/null; then
        realpath "$path" 2>/dev/null || echo "$path"
    elif command -v greadlink &>/dev/null; then
        greadlink -f "$path" 2>/dev/null || echo "$path"
    else
        # Pure bash fallback
        local dir base
        if [[ -d "$path" ]]; then
            (cd "$path" && pwd)
        elif [[ -f "$path" ]]; then
            dir=$(dirname "$path")
            base=$(basename "$path")
            (cd "$dir" && echo "$(pwd)/$base")
        else
            echo "$path"
        fi
    fi
}

# Format file size for display
format_size() {
    local bytes=$1
    if [[ $bytes -ge 1048576 ]]; then
        printf "%.1f MB" "$(echo "scale=1; $bytes/1048576" | bc 2>/dev/null || echo "$((bytes/1048576))")"
    elif [[ $bytes -ge 1024 ]]; then
        printf "%.1f KB" "$(echo "scale=1; $bytes/1024" | bc 2>/dev/null || echo "$((bytes/1024))")"
    else
        printf "%d bytes" "$bytes"
    fi
}

# Format elapsed time
format_time() {
    local seconds=$1
    if [[ $seconds -lt 60 ]]; then
        printf "%ds" "$seconds"
    else
        local mins=$((seconds / 60))
        local secs=$((seconds % 60))
        printf "%dm %ds" "$mins" "$secs"
    fi
}

# ============================================================================
# ARGUMENT PARSING
# ============================================================================
while [[ $# -gt 0 ]]; do
    case $1 in
        --input)
            INPUT_FILE="$2"
            shift 2
            ;;
        --output)
            OUTPUT_FILE="$2"
            shift 2
            ;;
        --help|-h)
            print_help
            exit 0
            ;;
        *)
            echo -e "${RED}ERROR: Unknown argument: $1${NC}"
            echo "Use --help for usage information."
            exit 1
            ;;
    esac
done

# Validate required arguments
if [[ -z "$INPUT_FILE" ]]; then
    echo -e "${RED}ERROR: --input argument is required. Use --help for usage.${NC}"
    exit 1
fi

if [[ -z "$OUTPUT_FILE" ]]; then
    echo -e "${RED}ERROR: --output argument is required. Use --help for usage.${NC}"
    exit 1
fi

# Convert to absolute paths using portable method
INPUT_FILE="$(get_realpath "$INPUT_FILE")"
OUTPUT_FILE="$(get_realpath "$OUTPUT_FILE")"

# Validate input file exists
if [[ ! -f "$INPUT_FILE" ]]; then
    echo -e "${RED}ERROR: Input file not found: $INPUT_FILE${NC}"
    exit 1
fi

# Create output directory if needed
OUTPUT_DIR="$(dirname "$OUTPUT_FILE")"
mkdir -p "$OUTPUT_DIR"

# ============================================================================
# MAIN PROCESSING WITH AWK (10-100x FASTER)
# ============================================================================
print_banner

echo -e "${YELLOW}Configuration:${NC}"
echo "  Input:   $INPUT_FILE"
echo "  Output:  $OUTPUT_FILE"

# Get file size for display (cross-platform)
file_size=$(stat -f%z "$INPUT_FILE" 2>/dev/null || stat -c%s "$INPUT_FILE" 2>/dev/null || echo "0")
echo -e "  Size:    $(format_size "$file_size")"
echo ""

# Count total rows first for accurate progress (fast grep count)
echo -e "${CYAN}Analyzing HTML structure...${NC}"
total_rows=$(grep -c '<tr' "$INPUT_FILE" 2>/dev/null || echo "0")
echo -e "  Found approximately ${WHITE}$total_rows${NC} table rows to process"
echo ""

# Warn for large files
if [[ $file_size -gt 52428800 ]]; then  # > 50MB
    echo -e "${YELLOW}⚠ Large file detected. Processing may take a moment...${NC}"
    echo ""
fi

echo -e "${GRAY}───────────────────────────────────────────────────────────────${NC}"
echo -e "${WHITE}Starting conversion...${NC}"
echo -e "${GRAY}───────────────────────────────────────────────────────────────${NC}"
echo ""

START_TIME=$(date +%s)

# Write CSV header
echo 'id,date_utc,media_type,latitude,longitude,download_url' > "$OUTPUT_FILE"

# Create a temporary stats file
STATS_FILE=$(mktemp)
trap "rm -f $STATS_FILE" EXIT

# Use awk for extremely fast HTML parsing
# NOTE: Uses POSIX-compliant awk (no gawk-specific features)
awk '
BEGIN {
    RS = "<tr"
    FS = "<td"
    rows_written = 0
    rows_skipped = 0
    srand()
}

function generate_uuid(    hex, i, c) {
    hex = ""
    for (i = 0; i < 32; i++) {
        c = int(rand() * 16)
        if (c < 10) hex = hex c
        else hex = hex substr("abcdef", c - 9, 1)
    }
    return substr(hex,1,8) "-" substr(hex,9,4) "-4" substr(hex,14,3) "-a" substr(hex,18,3) "-" substr(hex,21,12)
}

function strip_tags(str) {
    gsub(/<[^>]*>/, "", str)
    gsub(/^[ \t\r\n]+/, "", str)
    gsub(/[ \t\r\n]+$/, "", str)
    return str
}

function html_decode(str) {
    gsub(/&amp;/, "\\&", str)
    gsub(/&lt;/, "<", str)
    gsub(/&gt;/, ">", str)
    gsub(/&quot;/, "\"", str)
    gsub(/&#39;/, "'"'"'", str)
    gsub(/&apos;/, "'"'"'", str)
    gsub(/&#x27;/, "'"'"'", str)
    gsub(/&#x2F;/, "/", str)
    gsub(/&nbsp;/, " ", str)
    return str
}

function escape_csv(field) {
    if (index(field, ",") || index(field, "\"") || index(field, "\n")) {
        gsub(/"/, "\"\"", field)
        return "\"" field "\""
    }
    return field
}

function extract_cell_content(cell,    pos1, pos2, content) {
    pos1 = index(cell, ">")
    if (pos1 == 0) return strip_tags(cell)
    content = substr(cell, pos1 + 1)
    pos2 = index(content, "</td")
    if (pos2 > 0) content = substr(content, 1, pos2 - 1)
    return strip_tags(content)
}

function extract_coords(loc, which,    tmp, pos1, pos2, num) {
    if (index(loc, "Latitude") == 0) return ""
    pos1 = index(loc, ":")
    if (pos1 == 0) return ""
    tmp = substr(loc, pos1 + 1)
    gsub(/^[ \t]+/, "", tmp)
    pos2 = index(tmp, ",")
    if (pos2 == 0) return ""
    if (which == "lat") {
        num = substr(tmp, 1, pos2 - 1)
    } else {
        num = substr(tmp, pos2 + 1)
    }
    gsub(/^[ \t]+/, "", num)
    gsub(/[ \t]+$/, "", num)
    return num
}

function extract_download_url(row,    pos, tmp, url, endpos, sq) {
    pos = index(row, "downloadMemories")
    if (pos == 0) return ""
    tmp = substr(row, pos)
    
    sq = "'"'"'"
    pos = index(tmp, "(" sq)
    if (pos > 0) {
        tmp = substr(tmp, pos + 2)
        endpos = index(tmp, sq)
        if (endpos > 0) {
            url = substr(tmp, 1, endpos - 1)
            return html_decode(url)
        }
    }
    
    pos = index(tmp, "(\"")
    if (pos > 0) {
        tmp = substr(tmp, pos + 2)
        endpos = index(tmp, "\"")
        if (endpos > 0) {
            url = substr(tmp, 1, endpos - 1)
            return html_decode(url)
        }
    }
    
    pos = index(tmp, "(&quot;")
    if (pos > 0) {
        tmp = substr(tmp, pos + 7)
        endpos = index(tmp, "&quot;")
        if (endpos > 0) {
            url = substr(tmp, 1, endpos - 1)
            return html_decode(url)
        }
    }
    
    return ""
}

NR > 1 {
    date_utc = ""
    media_type = ""
    location = ""
    
    for (i = 2; i <= NF && i <= 5; i++) {
        content = extract_cell_content($i)
        if (i == 2) date_utc = content
        else if (i == 3) media_type = content
        else if (i == 4) location = content
    }
    
    if (media_type != "Image" && media_type != "Video") next
    
    latitude = extract_coords(location, "lat")
    longitude = extract_coords(location, "long")
    download_url = extract_download_url($0)
    
    if (download_url == "") {
        rows_skipped++
        next
    }
    
    id = generate_uuid()
    print escape_csv(id) "," escape_csv(date_utc) "," escape_csv(media_type) "," escape_csv(latitude) "," escape_csv(longitude) "," escape_csv(download_url)
    rows_written++
    
    if (rows_written % 100 == 0) {
        printf "\r  Processing: %d rows...", rows_written > "/dev/stderr"
    }
}

END {
    printf "\n" > "/dev/stderr"
    print rows_written ":" rows_skipped > "'"$STATS_FILE"'"
}
' "$INPUT_FILE" >> "$OUTPUT_FILE"

# Read stats
if [[ -f "$STATS_FILE" ]]; then
    IFS=':' read -r rows_written rows_skipped < "$STATS_FILE"
else
    rows_written=$(($(wc -l < "$OUTPUT_FILE") - 1))
    rows_skipped=0
fi

END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))

echo ""
echo -e "${GRAY}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}                    CONVERSION COMPLETE                        ${NC}"
echo -e "${GRAY}═══════════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "  ${WHITE}Rows written:${NC}  ${GREEN}${rows_written:-0}${NC}"
if [[ ${rows_skipped:-0} -gt 0 ]]; then
    echo -e "  ${WHITE}Rows skipped:${NC}  ${YELLOW}${rows_skipped}${NC} (missing download URL)"
fi
echo -e "  ${WHITE}Time elapsed:${NC}  $(format_time $ELAPSED)"
if [[ $ELAPSED -gt 0 && ${rows_written:-0} -gt 0 ]]; then
    speed=$((rows_written / ELAPSED))
    if [[ $speed -gt 0 ]]; then
        echo -e "  ${WHITE}Speed:${NC}         ${speed} rows/sec"
    fi
fi
echo ""
echo -e "  ${WHITE}Output file:${NC}   ${CYAN}$OUTPUT_FILE${NC}"
echo ""
