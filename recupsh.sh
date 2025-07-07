#!/bin/bash

# Script to download files from HDFS through Knox Gateway directly to target directory
# Usage: ./recup.sh <app_name> <hdfs_base_path> <date_range> <target_dir>
# Example: ./recup.sh myapp "/prd/logs" "20250401-20250601" "/target/directory"

set -e

#------------------------------------------------------------------------------
# Configuration
#------------------------------------------------------------------------------
KNOX_HOST="web:9443"
KNOX_BASE_PATH="/gateway/cdp-proxy-api/webhdfs/v1"
LOG_DIR="/var/log/hdfs_downloads"
LOG_RETENTION_DAYS=7

SPLUNK_USER="splunk"
SPLUNK_GROUP="splunk"

KNOX_USER="your_username_here"
KNOX_PASSWORD="your_password_here"

# Generate a unique process ID for logging purposes
PROCESS_ID="$$_$(date +%s%N)"

#------------------------------------------------------------------------------
# Function to validate date format
#------------------------------------------------------------------------------
validate_date() {
    local date_str="$1"
    if [[ ! "$date_str" =~ ^[0-9]{8}$ ]]; then
        log_message "ERROR" "Invalid date format. Expected YYYYMMDD" "\"date\":\"${date_str}\""
        return 1
    fi
    return 0
}

#------------------------------------------------------------------------------
# Function to parse date range
#------------------------------------------------------------------------------
parse_date_range() {
    local date_range="$1"
    if [[ "$date_range" =~ ^([0-9]{8})-([0-9]{8})$ ]]; then
        START_DATE="${BASH_REMATCH[1]}"
        END_DATE="${BASH_REMATCH[2]}"
        
        if ! validate_date "$START_DATE" || ! validate_date "$END_DATE"; then
            return 1
        fi
        
        # Check if start date is before or equal to end date
        if [ "$START_DATE" -gt "$END_DATE" ]; then
            log_message "ERROR" "Start date must be before or equal to end date" "\"start\":\"$START_DATE\",\"end\":\"$END_DATE\""
            return 1
        fi
        
        return 0
    else
        log_message "ERROR" "Invalid date range format. Expected YYYYMMDD-YYYYMMDD" "\"range\":\"$date_range\""
        return 1
    fi
}

#------------------------------------------------------------------------------
# Function to generate date list between start and end dates
#------------------------------------------------------------------------------
generate_date_list() {
    local start_date="$1"
    local end_date="$2"
    local current_date="$start_date"
    
    while [ "$current_date" -le "$end_date" ]; do
        echo "$current_date"
        # Increment date by one day
        current_date=$(date -d "${current_date:0:4}-${current_date:4:2}-${current_date:6:2} + 1 day" +%Y%m%d)
    done
}

#------------------------------------------------------------------------------
# Function to validate target directory
#------------------------------------------------------------------------------
validate_target_dir() {
    local dir="$1"
    if [ ! -d "$dir" ]; then
        if ! mkdir -p "$dir" 2>/dev/null; then
            log_message "ERROR" "Cannot create target directory" "\"directory\":\"${dir}\""
            return 1
        fi
    fi
    if [ ! -w "$dir" ]; then
        log_message "ERROR" "Target directory is not writable" "\"directory\":\"${dir}\""
        return 1
    fi
    return 0
}

#------------------------------------------------------------------------------
# Setup logging
#------------------------------------------------------------------------------
setup_logging() {
    local app="$1"
    
    mkdir -p "$LOG_DIR"
    chown "${SPLUNK_USER}:${SPLUNK_GROUP}" "$LOG_DIR"
    
    # Define log file with app name and process ID
    LOG_FILE="${LOG_DIR}/${app}_${PROCESS_ID}.log"
    
    touch "$LOG_FILE"
    chown "${SPLUNK_USER}:${SPLUNK_GROUP}" "$LOG_FILE"
    
    # Rotate old logs (older than LOG_RETENTION_DAYS)
    find "$LOG_DIR" -name "${app}_*.log" -type f -mtime +"${LOG_RETENTION_DAYS}" -delete 2>/dev/null || true
}

#------------------------------------------------------------------------------
# Function to log messages with JSON format
#------------------------------------------------------------------------------
log_message() {
    local level="$1"
    local message="$2"
    local additional_fields="$3"
    
    local timestamp
    timestamp=$(date -u '+%Y-%m-%dT%H:%M:%S.000Z')
    
    local log_entry="{\"timestamp\":\"${timestamp}\",\"level\":\"${level}\",\"app\":\"${APP_NAME}\",\"process_id\":\"${PROCESS_ID}\",\"message\":\"${message}\""
    
    if [ -n "$additional_fields" ]; then
        log_entry="${log_entry},${additional_fields}"
    fi
    
    log_entry="${log_entry}}"
    
    # Write to local log file
    echo "$log_entry" >> "$LOG_FILE"
    
    # Also output to stdout (for Control-M or general visibility)
    echo "$log_entry"
}

#------------------------------------------------------------------------------
# Function to get file size from HDFS
#------------------------------------------------------------------------------
get_file_size() {
    local file_path="$1"
    local size
    size=$(curl -s -k -I -u "${KNOX_USER}:${KNOX_PASSWORD}" \
        "https://${KNOX_HOST}${KNOX_BASE_PATH}${file_path}?op=GETFILESTATUS" \
        | grep -i "Content-Length" \
        | awk '{print $2}' \
        | tr -d '\r')
    
    echo "${size:-0}"
}

#------------------------------------------------------------------------------
# Function to format file size
#------------------------------------------------------------------------------
format_size() {
    local size="$1"
    if [ "$size" -ge 1073741824 ]; then
        echo "$(awk "BEGIN {printf \"%.2f\", $size/1073741824}")GB"
    elif [ "$size" -ge 1048576 ]; then
        echo "$(awk "BEGIN {printf \"%.2f\", $size/1048576}")MB"
    elif [ "$size" -ge 1024 ]; then
        echo "$(awk "BEGIN {printf \"%.2f\", $size/1024}")KB"
    else
        echo "${size}B"
    fi
}

#------------------------------------------------------------------------------
# Function to download a file from HDFS and place it directly in target directory
#------------------------------------------------------------------------------
download_file() {
    local file_path="$1"
    local date_processed="$2"
    local filename
    filename=$(basename "$file_path")
    
    local temp_file="${TEMP_DIR}/${filename}"
    local final_file="${TARGET_DIR}/${filename}"
    
    # Skip if file already exists
    if [ -f "$final_file" ]; then
        log_message "INFO" "File already exists" "\"file\":\"${filename}\",\"date\":\"${date_processed}\",\"status\":\"skipped\""
        return 0
    fi
    
    local file_size
    file_size=$(get_file_size "$file_path")
    local formatted_size
    formatted_size=$(format_size "$file_size")
    
    log_message "INFO" "Starting download" "\"file\":\"${filename}\",\"date\":\"${date_processed}\",\"path\":\"${file_path}\",\"size\":\"${formatted_size}\""
    
    local start_time
    start_time=$(date +%s)
    
    curl -s -k -L -o "$temp_file" \
         -u "${KNOX_USER}:${KNOX_PASSWORD}" \
         "https://${KNOX_HOST}${KNOX_BASE_PATH}${file_path}?op=OPEN"
    
    local end_time
    end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    # Check if download was successful and file is not empty
    if [ $? -ne 0 ] || [ ! -s "$temp_file" ]; then
        log_message "ERROR" "Download failed" "\"file\":\"${filename}\",\"date\":\"${date_processed}\",\"path\":\"${file_path}\""
        rm -f "$temp_file"
        return 1
    fi
    
    chown "${SPLUNK_USER}:${SPLUNK_GROUP}" "$temp_file"
    chmod 644 "$temp_file"
    
    # Move file into final location
    mv "$temp_file" "$final_file"
    chown "${SPLUNK_USER}:${SPLUNK_GROUP}" "$final_file"
    chmod 644 "$final_file"
    
    log_message "INFO" "Download completed" "\"file\":\"${filename}\",\"date\":\"${date_processed}\",\"path\":\"${file_path}\",\"duration_seconds\":${duration}"
    return 0
}

#------------------------------------------------------------------------------
# Function to process a single date
#------------------------------------------------------------------------------
process_date() {
    local date_to_process="$1"
    local hdfs_path="${HDFS_BASE_PATH}/${date_to_process}"
    
    log_message "INFO" "Processing date" "\"date\":\"${date_to_process}\",\"path\":\"${hdfs_path}\""
    
    local files_list
    files_list=$(curl -s -k -L \
        -u "${KNOX_USER}:${KNOX_PASSWORD}" \
        "https://${KNOX_HOST}${KNOX_BASE_PATH}${hdfs_path}?op=LISTSTATUS" \
        | grep -o '"pathSuffix":"[^"]*"' \
        | cut -d'"' -f4)
    
    if [ -z "$files_list" ]; then
        log_message "WARN" "No files found for date" "\"date\":\"${date_to_process}\",\"path\":\"${hdfs_path}\""
        return 0
    fi
    
    local date_success=0
    local date_total=0
    
    while IFS= read -r file; do
        if [ -n "$file" ]; then
            date_total=$((date_total + 1))
            if download_file "${hdfs_path}/${file}" "${date_to_process}"; then
                date_success=$((date_success + 1))
            fi
        fi
    done <<< "$files_list"
    
    log_message "INFO" "Date processing completed" "\"date\":\"${date_to_process}\",\"total_files\":${date_total},\"successful\":${date_success}"
    
    return $([ $date_success -eq $date_total ])
}

#------------------------------------------------------------------------------
# Cleanup function
#------------------------------------------------------------------------------
cleanup() {
    # Remove temporary directory
    rm -rf "$TEMP_DIR"
}

#------------------------------------------------------------------------------
# Main function
#------------------------------------------------------------------------------
main() {
    log_message "INFO" "Starting HDFS download process" "\"app\":\"${APP_NAME}\",\"hdfs_path\":\"${HDFS_BASE_PATH}\",\"date_range\":\"${DATE_RANGE}\",\"target_dir\":\"${TARGET_DIR}\""
    
    # Generate list of dates to process
    local dates_to_process
    dates_to_process=$(generate_date_list "$START_DATE" "$END_DATE")
    
    local total_success=0
    local total_files=0
    local processed_dates=0
    
    while IFS= read -r date_to_process; do
        if [ -n "$date_to_process" ]; then
            processed_dates=$((processed_dates + 1))
            if process_date "$date_to_process"; then
                total_success=$((total_success + 1))
            fi
        fi
    done <<< "$dates_to_process"
    
    log_message "INFO" "Process completed" "\"processed_dates\":${processed_dates},\"successful_dates\":${total_success}"
    
    if [ $total_success -ne $processed_dates ]; then
        log_message "ERROR" "Some dates failed processing" "\"failed_dates\":$((processed_dates - total_success))"
        exit 1
    fi
}

#------------------------------------------------------------------------------
# Parse input parameters
#------------------------------------------------------------------------------
if [ $# -ne 4 ]; then
    echo "Usage: $0 <app_name> <hdfs_base_path> <date_range> <target_dir>"
    echo "Example: $0 myapp \"/prd/logs\" \"20250401-20250601\" \"/target/directory\""
    exit 1
fi

APP_NAME="$1"
HDFS_BASE_PATH="$2"
DATE_RANGE="$3"
TARGET_DIR="$4"

# Parse and validate date range
if ! parse_date_range "$DATE_RANGE"; then
    exit 1
fi

# Validate target directory
if ! validate_target_dir "$TARGET_DIR"; then
    exit 1
fi

# Prepare logging
setup_logging "$APP_NAME"

# Create temporary directory for downloads
TEMP_DIR="/tmp/hdfs_download_${PROCESS_ID}"
mkdir -p "$TEMP_DIR"
chown -R "${SPLUNK_USER}:${SPLUNK_GROUP}" "$TEMP_DIR"

trap cleanup EXIT

# Execute main logic
main
