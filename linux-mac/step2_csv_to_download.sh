#!/bin/bash
#
# step2_csv_to_download.sh - SSLAXSnap v1.0 - Snapchat Memories Downloader
#
# OPTIMIZED VERSION with:
#   - O(1) associative array lookups (was O(n²))
#   - Single-pass CSV parsing (was 6x cut calls)
#   - Curl timeouts to prevent hanging
#   - Graceful Ctrl+C handling with progress save
#   - Visual progress bars, ETA, download speed
#   - Spinner animations
#
# Usage:
#   ./step2_csv_to_download.sh --input <path> --outdir <path> [--batch <int>] [--cooldown <seconds>]
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
readonly DIM='\033[2m'
readonly NC='\033[0m'

# Progress bar settings
readonly PROG_FILL='━'
readonly PROG_EMPTY='─'
readonly PROG_WIDTH=35

# Spinner frames
readonly SPINNER_FRAMES=('⠋' '⠙' '⠹' '⠸' '⠼' '⠴' '⠦' '⠧' '⠇' '⠏')
SPINNER_IDX=0

# ============================================================================
# DEFAULT VALUES
# ============================================================================
INPUT_FILE=""
OUTPUT_DIR=""
BATCH_SIZE=5
COOLDOWN_SECONDS=3
CURL_CONNECT_TIMEOUT=15
CURL_MAX_TIME=180

# ============================================================================
# GLOBAL STATE
# ============================================================================
SUCCESS_COUNT=0
FAILED_COUNT=0
SEQUENCE_COUNTER=0
TOTAL_BYTES=0
INTERRUPTED=0

# Declare associative array for O(1) lookups
declare -A SUCCESSFUL_MAP

# Store CSV lines in array
declare -a CSV_LINES

# ============================================================================
# BANNER
# ============================================================================
print_banner() {
    echo -e "${CYAN}"
    echo "╔═══════════════════════════════════════════════════════════════════════════╗"
    echo "║                                                                           ║"
    echo "║   ███████╗███████╗██╗      █████╗ ██╗  ██╗███████╗███╗   ██╗ █████╗ ██████╗║"
    echo "║   ██╔════╝██╔════╝██║     ██╔══██╗╚██╗██╔╝██╔════╝████╗  ██║██╔══██╗██╔══██║"
    echo "║   ███████╗███████╗██║     ███████║ ╚███╔╝ ███████╗██╔██╗ ██║███████║██████╔╝"
    echo "║   ╚════██║╚════██║██║     ██╔══██║ ██╔██╗ ╚════██║██║╚██╗██║██╔══██║██╔═══╝ ║"
    echo "║   ███████║███████║███████╗██║  ██║██╔╝ ██╗███████║██║ ╚████║██║  ██║██║     ║"
    echo "║   ╚══════╝╚══════╝╚══════╝╚═╝  ╚═╝╚═╝  ╚═╝╚══════╝╚═╝  ╚═══╝╚═╝  ╚═╝╚═╝     ║"
    echo "║                                                                           ║"
    echo "║                          SSLAXSnap v1.0                                   ║"
    echo "║           Snapchat Memories Batch Downloader (Optimized)                  ║"
    echo "║                       Linux/macOS Edition                                 ║"
    echo "║                                                                           ║"
    echo "╚═══════════════════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

# ============================================================================
# HELP
# ============================================================================
print_help() {
    echo "SSLAXSnap v1.0 - Snapchat Memories Downloader (Linux/macOS)"
    echo ""
    echo "USAGE:"
    echo "    ./step2_csv_to_download.sh --input <path> --outdir <path> [OPTIONS]"
    echo ""
    echo "REQUIRED ARGUMENTS:"
    echo "    --input <path>      Path to CSV input file"
    echo "    --outdir <path>     Root output folder for downloaded files"
    echo ""
    echo "OPTIONS:"
    echo "    --batch <int>       Max concurrent downloads (default: 5)"
    echo "    --cooldown <sec>    Delay after each batch in seconds (default: 3)"
    echo "    --help              Show this help message"
    echo ""
    echo "EXAMPLE:"
    echo "    ./step2_csv_to_download.sh --input snap_data.csv --outdir ./SnapDownloads --batch 10"
}

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

# Portable realpath
get_realpath() {
    local path="$1"
    if command -v realpath &>/dev/null; then
        realpath "$path" 2>/dev/null || echo "$path"
    elif command -v greadlink &>/dev/null; then
        greadlink -f "$path" 2>/dev/null || echo "$path"
    else
        if [[ -d "$path" ]]; then
            (cd "$path" && pwd)
        elif [[ -f "$path" ]]; then
            local dir base
            dir=$(dirname "$path")
            base=$(basename "$path")
            (cd "$dir" && echo "$(pwd)/$base")
        else
            echo "$path"
        fi
    fi
}

# Get current time in milliseconds
get_time_ms() {
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS: use perl for milliseconds
        perl -MTime::HiRes=time -e 'printf "%.0f\n", time()*1000' 2>/dev/null || echo "$(($(date +%s) * 1000))"
    else
        date +%s%3N 2>/dev/null || echo "$(($(date +%s) * 1000))"
    fi
}

# Format bytes for display
format_bytes() {
    local bytes=$1
    if [[ $bytes -ge 1073741824 ]]; then
        printf "%.2f GB" "$(echo "scale=2; $bytes/1073741824" | bc 2>/dev/null || echo "$((bytes/1073741824))")"
    elif [[ $bytes -ge 1048576 ]]; then
        printf "%.1f MB" "$(echo "scale=1; $bytes/1048576" | bc 2>/dev/null || echo "$((bytes/1048576))")"
    elif [[ $bytes -ge 1024 ]]; then
        printf "%.1f KB" "$(echo "scale=1; $bytes/1024" | bc 2>/dev/null || echo "$((bytes/1024))")"
    else
        printf "%d B" "$bytes"
    fi
}

# Format elapsed time
format_elapsed() {
    local ms=$1
    local seconds=$((ms / 1000))
    
    if [[ $seconds -lt 1 ]]; then
        echo "${ms}ms"
    elif [[ $seconds -lt 60 ]]; then
        echo "${seconds}s"
    elif [[ $seconds -lt 3600 ]]; then
        local mins=$((seconds / 60))
        local secs=$((seconds % 60))
        echo "${mins}m ${secs}s"
    else
        local hours=$((seconds / 3600))
        local mins=$(( (seconds % 3600) / 60 ))
        echo "${hours}h ${mins}m"
    fi
}

# Format time for display
format_time() {
    local seconds=$1
    if [[ $seconds -lt 60 ]]; then
        printf "%ds" "$seconds"
    elif [[ $seconds -lt 3600 ]]; then
        printf "%dm %ds" "$((seconds/60))" "$((seconds%60))"
    else
        printf "%dh %dm" "$((seconds/3600))" "$(((seconds%3600)/60))"
    fi
}

# Render progress bar
render_progress_bar() {
    local current=$1
    local total=$2
    local start_time=$3
    
    [[ $total -eq 0 ]] && return
    
    local percent=$((current * 100 / total))
    local filled=$((current * PROG_WIDTH / total))
    local empty=$((PROG_WIDTH - filled))
    
    # Build bar
    local bar=""
    for ((i=0; i<filled; i++)); do bar+="$PROG_FILL"; done
    for ((i=0; i<empty; i++)); do bar+="$PROG_EMPTY"; done
    
    # Calculate ETA
    local now eta_str speed_str
    now=$(date +%s)
    local elapsed=$((now - start_time))
    
    if [[ $current -gt 0 && $elapsed -gt 0 ]]; then
        local avg_per_item=$((elapsed * 1000 / current))  # ms per item
        local remaining_items=$((total - current))
        local remaining_time=$((remaining_items * avg_per_item / 1000))
        eta_str="ETA: $(format_time $remaining_time)"
        
        if [[ $TOTAL_BYTES -gt 0 ]]; then
            local bytes_per_sec=$((TOTAL_BYTES / elapsed))
            speed_str="$(format_bytes $bytes_per_sec)/s"
        fi
    else
        eta_str="ETA: --"
        speed_str=""
    fi
    
    printf "\r  ${CYAN}[${bar}]${NC} %3d%% ${GRAY}│${NC} %d/${total} ${GRAY}│${NC} ${eta_str} ${GRAY}│${NC} ${speed_str}   " \
        "$percent" "$current"
}

# Get spinner frame
get_spinner() {
    local frame=$((SPINNER_IDX % ${#SPINNER_FRAMES[@]}))
    SPINNER_IDX=$((SPINNER_IDX + 1))
    echo "${SPINNER_FRAMES[$frame]}"
}

# Detect file format from magic bytes (optimized)
get_file_format() {
    local file_path="$1"
    
    [[ ! -f "$file_path" ]] && echo "jpeg" && return
    
    # Read first 12 bytes as hex
    local hex_bytes
    hex_bytes=$(od -A n -t x1 -N 12 "$file_path" 2>/dev/null | tr -d ' \n' | tr '[:upper:]' '[:lower:]')
    
    # Check patterns in order of likelihood
    case "$hex_bytes" in
        ffd8ff*)         echo "jpeg"; return ;;  # JPEG
        89504e470d0a1a0a*) echo "png"; return ;; # PNG
        504b*)           echo "zip"; return ;;   # ZIP
        *66747970*)      echo "mp4"; return ;;   # MP4 (ftyp)
    esac
    
    # Fallback to file command
    local mime
    mime=$(file -b --mime-type "$file_path" 2>/dev/null)
    case "$mime" in
        *jpeg*|*jpg*)    echo "jpeg" ;;
        *png*)           echo "png" ;;
        *zip*|*archive*) echo "zip" ;;
        *mp4*|*video*)   echo "mp4" ;;
        *)               echo "jpeg" ;;
    esac
}

# Get file extension for format
get_extension() {
    case "$1" in
        zip)  echo ".zip" ;;
        png)  echo ".png" ;;
        mp4)  echo ".mp4" ;;
        *)    echo ".jpg" ;;
    esac
}

# Get type label for filename
get_type_label() {
    case "$1" in
        zip)  echo "filter_archive" ;;
        mp4)  echo "video" ;;
        *)    echo "image" ;;
    esac
}

# Parse date and return folder path
get_date_folder() {
    local date_utc="$1"
    local date_part="${date_utc% UTC}"
    
    local month_names=("" "January" "February" "March" "April" "May" "June" 
                       "July" "August" "September" "October" "November" "December")
    
    if [[ "$date_part" =~ ([0-9]{4})-([0-9]{2})-([0-9]{2}) ]]; then
        local year="${BASH_REMATCH[1]}"
        local month="${BASH_REMATCH[2]}"
        local day="${BASH_REMATCH[3]}"
        local month_num=$((10#$month))
        echo "${year}/${month} ${month_names[$month_num]}/${day}.${month}.${year}"
    else
        echo "0000/00 Unknown/00.00.0000"
    fi
}

# Parse date and return timestamp for filename
get_timestamp() {
    local date_utc="$1"
    local date_part="${date_utc% UTC}"
    
    if [[ "$date_part" =~ ([0-9]{4})-([0-9]{2})-([0-9]{2})\ ([0-9]{2}):([0-9]{2}):([0-9]{2}) ]]; then
        echo "${BASH_REMATCH[3]}.${BASH_REMATCH[2]}.${BASH_REMATCH[1]}_${BASH_REMATCH[4]}${BASH_REMATCH[5]}${BASH_REMATCH[6]}"
    else
        echo "00.00.0000_000000"
    fi
}

# Save CSV atomically
save_csv_atomically() {
    local csv_path="$1"
    shift
    local rows=("$@")
    
    local temp_path="${csv_path}.tmp.$$"
    
    # Write header
    echo "id,date_utc,media_type,latitude,longitude,download_url" > "$temp_path"
    
    # Write remaining rows
    for row in "${rows[@]}"; do
        echo "$row" >> "$temp_path"
    done
    
    # Atomic move
    mv -f "$temp_path" "$csv_path"
}

# ============================================================================
# SIGNAL HANDLING - Graceful shutdown
# ============================================================================
cleanup_and_exit() {
    INTERRUPTED=1
    echo ""
    echo ""
    echo -e "${YELLOW}⚠ Interrupt received - saving progress...${NC}"
    
    # Kill all background jobs
    jobs -p | xargs -r kill 2>/dev/null
    
    # Save current CSV state if we have remaining rows
    if [[ ${#CSV_LINES[@]} -gt 0 ]]; then
        echo -e "${CYAN}Saving ${#CSV_LINES[@]} remaining items to CSV...${NC}"
        save_csv_atomically "$INPUT_FILE" "${CSV_LINES[@]}"
        echo -e "${GREEN}Progress saved.${NC}"
    fi
    
    # Print summary
    echo ""
    echo -e "${GRAY}═══════════════════════════════════════════════════════════════════════════${NC}"
    echo -e "${YELLOW}                         DOWNLOAD INTERRUPTED${NC}"
    echo -e "${GRAY}═══════════════════════════════════════════════════════════════════════════${NC}"
    echo ""
    echo -e "  ${WHITE}Completed:${NC}  ${GREEN}$SUCCESS_COUNT${NC}"
    echo -e "  ${WHITE}Failed:${NC}     ${RED}$FAILED_COUNT${NC}"
    echo -e "  ${WHITE}Remaining:${NC}  ${YELLOW}${#CSV_LINES[@]}${NC}"
    echo ""
    echo -e "  ${GRAY}Run again to resume from where you left off.${NC}"
    echo ""
    
    exit 130
}

# Set up signal traps
trap cleanup_and_exit SIGINT SIGTERM

# ============================================================================
# DOWNLOAD FUNCTION
# ============================================================================
download_item() {
    local id="$1"
    local date_utc="$2"
    local media_type="$3"
    local latitude="$4"
    local longitude="$5"
    local download_url="$6"
    local output_dir="$7"
    local sequence_num="$8"
    local result_file="$9"
    
    local start_time
    start_time=$(get_time_ms)
    
    # Create date folder
    local date_folder target_dir
    date_folder=$(get_date_folder "$date_utc")
    target_dir="${output_dir}/${date_folder}"
    mkdir -p "$target_dir"
    
    # Temp file
    local temp_file="${target_dir}/${id}.tmp"
    local curl_stderr="${temp_file}.err"
    
    # Download with curl - includes timeout
    local http_code
    http_code=$(curl -s -S -L \
        --connect-timeout "$CURL_CONNECT_TIMEOUT" \
        --max-time "$CURL_MAX_TIME" \
        -o "$temp_file" \
        -w "%{http_code}|%{size_download}" \
        "$download_url" 2>"$curl_stderr")
    
    local curl_exit=$?
    local response_code="${http_code%%|*}"
    local file_size="${http_code##*|}"
    
    # Read any curl error
    local curl_error=""
    if [[ -f "$curl_stderr" ]]; then
        curl_error=$(head -c 100 "$curl_stderr" | tr '\n' ' ')
        rm -f "$curl_stderr"
    fi
    
    local end_time
    end_time=$(get_time_ms)
    local elapsed=$((end_time - start_time))
    
    # Check for errors
    if [[ $curl_exit -ne 0 ]]; then
        rm -f "$temp_file"
        echo "FAIL|${id}|${curl_exit}|curl error: ${curl_error}|${elapsed}|0" > "$result_file"
        return 1
    fi
    
    if [[ ! "$response_code" =~ ^2[0-9]{2}$ ]]; then
        rm -f "$temp_file"
        echo "FAIL|${id}|${response_code}|HTTP ${response_code}|${elapsed}|0" > "$result_file"
        return 1
    fi
    
    if [[ ! -f "$temp_file" ]]; then
        echo "FAIL|${id}|${response_code}|File not created|${elapsed}|0" > "$result_file"
        return 1
    fi
    
    # Detect file format
    local file_format
    file_format=$(get_file_format "$temp_file")
    
    # Build filename
    local timestamp coords seq type_label ext final_name final_path
    timestamp=$(get_timestamp "$date_utc")
    coords="[lat_${latitude:-}_long_${longitude:-}]"
    seq=$(printf "%04d" "$sequence_num")
    type_label=$(get_type_label "$file_format")
    ext=$(get_extension "$file_format")
    
    final_name="${timestamp}_${coords}_${seq}_${type_label}${ext}"
    final_path="${target_dir}/${final_name}"
    
    # Rename file
    mv -f "$temp_file" "$final_path"
    
    echo "OK|${id}|${response_code}|${file_format}|${final_name}|${elapsed}|${file_size}" > "$result_file"
    return 0
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
        --outdir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --batch)
            BATCH_SIZE="$2"
            shift 2
            ;;
        --cooldown)
            COOLDOWN_SECONDS="$2"
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

# Validate arguments
if [[ -z "$INPUT_FILE" ]]; then
    echo -e "${RED}ERROR: --input argument is required. Use --help for usage.${NC}"
    exit 1
fi

if [[ -z "$OUTPUT_DIR" ]]; then
    echo -e "${RED}ERROR: --outdir argument is required. Use --help for usage.${NC}"
    exit 1
fi

# Convert to absolute paths
INPUT_FILE="$(get_realpath "$INPUT_FILE")"

# Create output directory and get absolute path
mkdir -p "$OUTPUT_DIR"
OUTPUT_DIR="$(cd "$OUTPUT_DIR" && pwd)"

# ============================================================================
# MAIN EXECUTION
# ============================================================================
print_banner

echo -e "${YELLOW}Configuration:${NC}"
echo "  Input CSV:   $INPUT_FILE"
echo "  Output Dir:  $OUTPUT_DIR"
echo "  Batch Size:  $BATCH_SIZE"
echo "  Cooldown:    ${COOLDOWN_SECONDS}s"
echo "  Timeouts:    connect=${CURL_CONNECT_TIMEOUT}s, max=${CURL_MAX_TIME}s"
echo ""

# Validate input file
if [[ ! -f "$INPUT_FILE" ]]; then
    echo -e "${RED}ERROR: Input file not found: $INPUT_FILE${NC}"
    exit 1
fi

# Load CSV (skip header, strip CRLF)
echo -e "${CYAN}Loading CSV...${NC}"
mapfile -t CSV_LINES < <(tail -n +2 "$INPUT_FILE" | tr -d '\r' | grep -v '^[[:space:]]*$')

TOTAL_ROWS=${#CSV_LINES[@]}
if [[ $TOTAL_ROWS -eq 0 ]]; then
    echo -e "${GREEN}✓ All items already downloaded. Nothing to do.${NC}"
    exit 0
fi

echo -e "${GREEN}Loaded ${WHITE}$TOTAL_ROWS${GREEN} rows to process${NC}"
echo ""

# Calculate batches
TOTAL_BATCHES=$(( (TOTAL_ROWS + BATCH_SIZE - 1) / BATCH_SIZE ))

echo -e "${GRAY}═══════════════════════════════════════════════════════════════════════════${NC}"
echo -e "${WHITE}Starting downloads: $TOTAL_ROWS items in $TOTAL_BATCHES batches${NC}"
echo -e "${GRAY}═══════════════════════════════════════════════════════════════════════════${NC}"
echo ""

OVERALL_START=$(date +%s)
CURRENT_BATCH=0

# Create temp directory for job results
TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR; cleanup_and_exit" EXIT

# ============================================================================
# BATCH PROCESSING LOOP
# ============================================================================
while [[ ${#CSV_LINES[@]} -gt 0 ]]; do
    [[ $INTERRUPTED -eq 1 ]] && break
    
    CURRENT_BATCH=$((CURRENT_BATCH + 1))
    
    # Get items for this batch
    items_to_take=$BATCH_SIZE
    if [[ ${#CSV_LINES[@]} -lt $BATCH_SIZE ]]; then
        items_to_take=${#CSV_LINES[@]}
    fi
    
    BATCH_ITEMS=("${CSV_LINES[@]:0:$items_to_take}")
    
    echo -e "${GRAY}───────────────────────────────────────────────────────────────────────────${NC}"
    echo -e "${CYAN}$(get_spinner) BATCH $CURRENT_BATCH/$TOTAL_BATCHES${NC} - Processing ${#BATCH_ITEMS[@]} items (${#CSV_LINES[@]} remaining)"
    echo -e "${GRAY}───────────────────────────────────────────────────────────────────────────${NC}"
    
    # Start background jobs
    declare -a PIDS=()
    declare -a BATCH_IDS=()
    
    for item in "${BATCH_ITEMS[@]}"; do
        SEQUENCE_COUNTER=$((SEQUENCE_COUNTER + 1))
        
        # Parse CSV line efficiently - single IFS split instead of 6 cut calls
        IFS=',' read -r id date_utc media_type latitude longitude rest <<< "$item"
        
        # Clean up quotes
        id="${id//\"/}"
        date_utc="${date_utc//\"/}"
        media_type="${media_type//\"/}"
        latitude="${latitude//\"/}"
        longitude="${longitude//\"/}"
        download_url="${rest//\"/}"
        download_url="${download_url//$'\r'/}"
        
        # Result file for this job
        result_file="${TEMP_DIR}/result_${id}"
        
        # Start download in background
        download_item "$id" "$date_utc" "$media_type" "$latitude" "$longitude" \
            "$download_url" "$OUTPUT_DIR" "$SEQUENCE_COUNTER" "$result_file" &
        
        PIDS+=($!)
        BATCH_IDS+=("$id")
    done
    
    # Wait for all jobs
    for pid in "${PIDS[@]}"; do
        wait "$pid" 2>/dev/null || true
    done
    
    # Collect results - O(1) lookup with associative array
    BATCH_SUCCESS=0
    BATCH_FAIL=0
    
    for id in "${BATCH_IDS[@]}"; do
        local result_file="${TEMP_DIR}/result_${id}"
        
        if [[ -f "$result_file" ]]; then
            local result
            result=$(cat "$result_file")
            rm -f "$result_file"
            
            IFS='|' read -r status item_id code info1 info2 elapsed file_size <<< "$result"
            
            local elapsed_fmt
            elapsed_fmt=$(format_elapsed "$elapsed")
            
            if [[ "$status" == "OK" ]]; then
                SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
                BATCH_SUCCESS=$((BATCH_SUCCESS + 1))
                TOTAL_BYTES=$((TOTAL_BYTES + file_size))
                
                # Mark as successful in associative array (O(1))
                SUCCESSFUL_MAP["$id"]=1
                
                local size_str=""
                if [[ $file_size -gt 0 ]]; then
                    size_str=" $(format_bytes $file_size)"
                fi
                
                echo -e "  ${GREEN}✓${NC} ${GRAY}${id:0:8}...${NC} ${GREEN}${info1}${NC}${size_str} ${GRAY}(${elapsed_fmt})${NC}"
            else
                FAILED_COUNT=$((FAILED_COUNT + 1))
                BATCH_FAIL=$((BATCH_FAIL + 1))
                SEQUENCE_COUNTER=$((SEQUENCE_COUNTER - 1))
                
                echo -e "  ${RED}✗${NC} ${GRAY}${id:0:8}...${NC} ${RED}${info1}${NC} ${GRAY}(${elapsed_fmt})${NC}"
            fi
        else
            FAILED_COUNT=$((FAILED_COUNT + 1))
            BATCH_FAIL=$((BATCH_FAIL + 1))
            SEQUENCE_COUNTER=$((SEQUENCE_COUNTER - 1))
            echo -e "  ${RED}✗${NC} ${GRAY}${id:0:8}...${NC} ${RED}No response${NC}"
        fi
    done
    
    # Remove successful items - O(n) instead of O(n²)
    declare -a NEW_CSV_LINES=()
    for line in "${CSV_LINES[@]}"; do
        local line_id="${line%%,*}"
        line_id="${line_id//\"/}"
        
        # O(1) associative array lookup
        if [[ -z "${SUCCESSFUL_MAP[$line_id]:-}" ]]; then
            NEW_CSV_LINES+=("$line")
        fi
    done
    
    # Handle empty array case properly
    if [[ ${#NEW_CSV_LINES[@]} -gt 0 ]]; then
        CSV_LINES=("${NEW_CSV_LINES[@]}")
    else
        CSV_LINES=()
    fi
    
    # Save CSV atomically if we had successes
    if [[ $BATCH_SUCCESS -gt 0 ]]; then
        if [[ ${#CSV_LINES[@]} -gt 0 ]]; then
            save_csv_atomically "$INPUT_FILE" "${CSV_LINES[@]}"
        else
            # All done - write header only
            echo "id,date_utc,media_type,latitude,longitude,download_url" > "$INPUT_FILE"
        fi
    fi
    
    # Batch summary
    echo ""
    local completed=$((SUCCESS_COUNT + FAILED_COUNT))
    render_progress_bar "$completed" "$TOTAL_ROWS" "$OVERALL_START"
    echo ""
    echo -e "  Batch $CURRENT_BATCH: ${GREEN}$BATCH_SUCCESS OK${NC}, ${RED}$BATCH_FAIL failed${NC} │ Total: ${GREEN}$SUCCESS_COUNT${NC}/${TOTAL_ROWS}"
    
    # Cooldown with countdown
    if [[ ${#CSV_LINES[@]} -gt 0 && $COOLDOWN_SECONDS -gt 0 ]]; then
        echo ""
        for ((i=COOLDOWN_SECONDS; i>0; i--)); do
            printf "\r  ${YELLOW}$(get_spinner) Cooldown: %ds remaining...${NC}   " "$i"
            sleep 1
        done
        printf "\r  ${GRAY}Cooldown complete.                    ${NC}\n"
    fi
    
    echo ""
done

OVERALL_END=$(date +%s)
TOTAL_TIME=$((OVERALL_END - OVERALL_START))

# ============================================================================
# FINAL SUMMARY
# ============================================================================
echo ""
echo -e "${GRAY}═══════════════════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}                           DOWNLOAD COMPLETE                              ${NC}"
echo -e "${GRAY}═══════════════════════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "  ${WHITE}Total Time:${NC}     $(format_time $TOTAL_TIME)"
echo -e "  ${WHITE}Total Items:${NC}    $TOTAL_ROWS"
echo -e "  ${WHITE}Successful:${NC}     ${GREEN}$SUCCESS_COUNT${NC}"
if [[ $FAILED_COUNT -gt 0 ]]; then
    echo -e "  ${WHITE}Failed:${NC}         ${RED}$FAILED_COUNT${NC}"
else
    echo -e "  ${WHITE}Failed:${NC}         ${GREEN}0${NC}"
fi
if [[ ${#CSV_LINES[@]} -gt 0 ]]; then
    echo -e "  ${WHITE}Remaining:${NC}      ${YELLOW}${#CSV_LINES[@]}${NC}"
else
    echo -e "  ${WHITE}Remaining:${NC}      ${GREEN}0${NC}"
fi
if [[ $TOTAL_BYTES -gt 0 ]]; then
    echo -e "  ${WHITE}Downloaded:${NC}     $(format_bytes $TOTAL_BYTES)"
fi
echo ""

if [[ ${#CSV_LINES[@]} -gt 0 ]]; then
    echo -e "  ${YELLOW}⚠ ${#CSV_LINES[@]} items still pending - run again to retry.${NC}"
    echo ""
fi

echo -e "  ${WHITE}Output folder:${NC}  ${CYAN}$OUTPUT_DIR${NC}"
echo ""
