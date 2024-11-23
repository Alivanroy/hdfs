#!/bin/bash

# Script: knox_hdfs_download.sh
# Description: Downloads files from HDFS through Knox Gateway with direct file rotation
# Usage: ./knox_hdfs_download.sh -a "app1,app2" -p "/path1,/path2" [-d YYYYMMDD]
# Example: 
# Single app, multiple paths:
#   ./knox_hdfs_download.sh -a "myapp" -p "/logs/path1,/logs/path2"
# Multiple apps and paths:
#   ./knox_hdfs_download.sh -a "app1,app2" -p "/path1,/path2" -d 20240121

###################
# Configuration
###################

# Knox settings
KNOX_HOST="web:9443"
KNOX_BASE_PATH="/gateway/cdp-proxy-api/webhdfs/v1"
KNOX_USER="your_username_here"
KNOX_PASSWORD="your_password_here"

# Directory settings
OUTPUT_DIR="/opt/splunk/data"
LOG_DIR="/var/log/hdfs_downloads"
LOCK_DIR="/var/lock/hdfs_downloads"

# Retention settings
LOG_RETENTION_DAYS=7
FILE_RETENTION_DAYS=30  # How long to keep downloaded files

# User settings
SPLUNK_USER="splunk"
SPLUNK_GROUP="splunk"

# Process identifier
PROCESS_ID="$$_$(date +%s%N)"

###################
# Utility Functions
###################

usage() {
    echo "Usage: $0 -a APP_NAME[,APP_NAME2,...] -p PATH[,PATH2,...] [-d DATE]"
    echo "  -a: Comma-separated list of app names"
    echo "  -p: Comma-separated list of HDFS paths"
    echo "  -d: Optional date in YYYYMMDD format"
    echo
    echo "Examples:"
    echo "  $0 -a myapp -p /logs/path1,/logs/path2"
    echo "  $0 -a app1,app2 -p /path1,/path2"
    echo "  $0 -a app1 -p /path1,/path2 -d 20240121"
    exit 1
}

log_message() {
    local app="$1"
    local level="$2"
    local message="$3"
    local extra_fields="$4"
    local timestamp=$(date -u '+%Y-%m-%dT%H:%M:%S.000Z')
    
    # Build JSON log entry
    local json_entry="{"
    json_entry+="\"timestamp\":\"${timestamp}\","
    json_entry+="\"level\":\"${level}\","
    json_entry+="\"app\":\"${app}\","
    json_entry+="\"process_id\":\"${PROCESS_ID}\","
    json_entry+="\"message\":\"${message}\""
    
    # Add date if exists
    [ ! -z "$DATE_PATH" ] && json_entry+=",\"date_path\":\"${DATE_PATH}\""
    
    # Add extra fields if provided
    [ ! -z "$extra_fields" ] && json_entry+=",${extra_fields}"
    
    json_entry+="}"
    
    # Write to log file atomically
    local log_file="${LOG_DIR}/${app}${DATE_PATH:+_$DATE_PATH}_${PROCESS_ID}.log"
    (
        flock -x 200
        echo "$json_entry" >> "$log_file"
    ) 200>"$log_file.lock"
    
    # Output to stdout for Control-M
    echo "$json_entry"
}

acquire_lock() {
    local lock_file="$1"
    local max_wait=300
    local wait_time=0
    
    while ! mkdir "$lock_file" 2>/dev/null; do
        wait_time=$((wait_time + 1))
        if [ $wait_time -ge $max_wait ]; then
            return 1
        fi
        sleep 1
    done
    
    chown -R ${SPLUNK_USER}:${SPLUNK_GROUP} "$lock_file"
    return 0
}

release_lock() {
    rm -rf "$1"
}

validate_date() {
    local date_str="$1"
    if [[ ! "$date_str" =~ ^[0-9]{8}$ ]]; then
        return 1
    fi
    return 0
}

###################
# Core Functions
###################

setup_app_directory() {
    local app="$1"
    local base_dir="${OUTPUT_DIR}/${app}"
    
    if [ ! -z "$DATE_PATH" ]; then
        local dir="${base_dir}/${DATE_PATH}"
    else
        local dir="${base_dir}"
    fi
    
    mkdir -p "$dir"
    chown ${SPLUNK_USER}:${SPLUNK_GROUP} "$dir"
    
    echo "$dir"
}

rotate_files() {
    local app="$1"
    local app_dir="${OUTPUT_DIR}/${app}"
    local rotation_lock="${LOCK_DIR}/${app}_rotation.lock"
    
    if ! acquire_lock "$rotation_lock"; then
        log_message "$app" "WARN" "Skipping rotation - could not acquire lock"
        return
    fi
    
    log_message "$app" "INFO" "Starting file rotation" "\"retention_days\":${FILE_RETENTION_DAYS}"
    
    if [ ! -z "$DATE_PATH" ]; then
        # Remove old dated directories
        find "$app_dir" -maxdepth 1 -type d -mtime +${FILE_RETENTION_DAYS} | while read dir; do
            if [[ "$dir" != "$app_dir" && "$dir" != "${app_dir}/temp" ]]; then
                local dir_date=$(basename "$dir")
                if [[ "$dir_date" =~ ^[0-9]{8}$ ]]; then
                    log_message "$app" "INFO" "Removing expired directory" "\"directory\":\"$dir\""
                    rm -rf "$dir"
                fi
            fi
        done
    else
        # Remove old files directly
        find "$app_dir" -type f -mtime +${FILE_RETENTION_DAYS} | while read file; do
            log_message "$app" "INFO" "Removing expired file" "\"file\":\"$(basename "$file")\""
            rm -f "$file"
        done
    fi
    
    release_lock "$rotation_lock"
}

download_file() {
    local app="$1"
    local hdfs_path="$2"
    local local_dir="$3"
    local filename="$4"
    local temp_dir="${OUTPUT_DIR}/${app}/temp/${PROCESS_ID}"
    
    mkdir -p "$temp_dir"
    chown ${SPLUNK_USER}:${SPLUNK_GROUP} "$temp_dir"
    
    local temp_file="${temp_dir}/${filename}"
    local final_file="${local_dir}/${filename}"
    
    # Skip if file exists
    if [ -f "$final_file" ]; then
        log_message "$app" "INFO" "File exists, skipping" "\"file\":\"$filename\""
        return 0
    fi
    
    log_message "$app" "INFO" "Starting download" "\"file\":\"$filename\",\"path\":\"$hdfs_path\""
    
    local start_time=$(date +%s)
    
    if ! curl -s -k -L -o "$temp_file" \
        -u "${KNOX_USER}:${KNOX_PASSWORD}" \
        "https://${KNOX_HOST}${KNOX_BASE_PATH}${hdfs_path}/${filename}?op=OPEN"; then
        log_message "$app" "ERROR" "Download failed" "\"file\":\"$filename\""
        rm -f "$temp_file"
        return 1
    fi
    
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    if mv "$temp_file" "$final_file"; then
        chown ${SPLUNK_USER}:${SPLUNK_GROUP} "$final_file"
        chmod 644 "$final_file"
        log_message "$app" "INFO" "Download completed" \
            "\"file\":\"$filename\",\"duration\":$duration"
        return 0
    else
        log_message "$app" "ERROR" "Failed to move file" "\"file\":\"$filename\""
        rm -f "$temp_file"
        return 1
    fi
}

process_path() {
    local app="$1"
    local hdfs_path="$2"
    local local_dir="$3"
    
    # List HDFS directory
    local files_list=$(curl -s -k -L \
        -u "${KNOX_USER}:${KNOX_PASSWORD}" \
        "https://${KNOX_HOST}${KNOX_BASE_PATH}${hdfs_path}?op=LISTSTATUS" | \
        grep -o '"pathSuffix":"[^"]*"' | cut -d'"' -f4)
    
    if [ -z "$files_list" ]; then
        log_message "$app" "ERROR" "No files found" "\"path\":\"${hdfs_path}\""
        return 1
    fi
    
    local total_files=0
    local success_count=0
    
    while IFS= read -r file; do
        if [ ! -z "$file" ]; then
            ((total_files++))
            download_file "$app" "$hdfs_path" "$local_dir" "$file" && ((success_count++))
        fi
    done <<< "$files_list"
    
    log_message "$app" "INFO" "Path processing completed" \
        "\"path\":\"${hdfs_path}\",\"total\":${total_files},\"successful\":${success_count}"
    
    return $([ $success_count -eq $total_files ])
}

cleanup() {
    for app in ${APPS//,/ }; do
        rm -rf "${OUTPUT_DIR}/${app}/temp/${PROCESS_ID}"
    done
}

###################
# Main Execution
###################

# Parse command line arguments
while getopts "a:p:d:" opt; do
    case $opt in
        a) APPS="$OPTARG" ;;
        p) PATHS="$OPTARG" ;;
        d) DATE_PATH="$OPTARG" ;;
        ?) usage ;;
    esac
done

# Validate inputs
[ -z "$APPS" ] && usage
[ -z "$PATHS" ] && usage

# Validate date if provided
if [ ! -z "$DATE_PATH" ] && ! validate_date "$DATE_PATH"; then
    echo "Error: Invalid date format. Expected YYYYMMDD"
    exit 1
fi

# Register cleanup
trap cleanup EXIT

# Process each app and path combination
success=0
failures=0

for app in ${APPS//,/ }; do
    # Setup app directory
    local_dir=$(setup_app_directory "$app")
    
    # Rotate files for this app
    rotate_files "$app"
    
    # Process each path for this app
    for path in ${PATHS//,/ }; do
        if process_path "$app" "$path" "$local_dir"; then
            ((success++))
        else
            ((failures++))
        fi
    done
done

# Exit with error if any path failed
[ $failures -eq 0 ] || exit 1
