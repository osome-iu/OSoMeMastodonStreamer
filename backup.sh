#!/bin/bash
export TZ=UTC

# Function to read values from config.yml using yq
read_config() {
    # usage: read_config '.key'
    yq "$1" config.yml 2>/dev/null || echo ""
}

# Function to send an email using mailx
send_email() {
    local subject="$1"
    local message="$2"
    if [ -n "$backup_email" ]; then
        echo -e "$message" | mailx -s "$subject" "$backup_email" || echo "Warning: mailx failed to send email to $backup_email" >&2
    else
        echo "No backup_email configured; cannot send: $subject" >&2
    fi
}

# Function to log messages with a timestamp
log_message() {
    local message="$1"
    local timestamp
    timestamp=$(date +"%Y-%m-%d %H:%M:%S")
    echo "[$timestamp] $message" | tee -a "$log_file"
}

# ---------------------------------------------------------------------
# Read base_folder, backup_folders (REQUIRED), log_folder, and backup_email
# from config.yml
# ---------------------------------------------------------------------
base_folder=$(read_config '.base_folder')
# Try to read YAML array backup_folders[]; we'll parse into shell array below
# Use yq to get raw list lines; fallback to empty
backup_folders_raw=$(yq '.backup_folders[]' config.yml 2>/dev/null || true)
log_folder=$(read_config '.log_folder')
backup_email=$(read_config '.backup_email')

# Trim quotes helper
trim_quotes() {
  sed 's/^"//' | sed 's/"$//'
}
base_folder=$(echo "$base_folder" | trim_quotes)
log_folder=$(echo "$log_folder" | trim_quotes)
backup_email=$(echo "$backup_email" | trim_quotes)

# Ensure the log_folder exists (we need log_file for logging)
if [ -z "$log_folder" ]; then
    # fallback to ./log if not set (but prefer config)
    log_folder="./log"
fi
mkdir -p "$log_folder"

# Get yesterday to form log_file name early
yesterday_tmp=$(date -u -d "yesterday" +%F 2>/dev/null || date -u -v -1d +%F 2>/dev/null || date -u +%F)
log_file="$log_folder/backup_log_$yesterday_tmp.txt"
touch "$log_file"

# Validate base_folder
if [ -z "$base_folder" ]; then
    log_message "Error: Could not read base_folder from config.yml."
    send_email "Backup Script Failure" "Error: Could not read base_folder from config.yml."
    exit 1
fi

# Parse backup_folders into an array; require at least one entry (multiple-only policy)
backup_roots=()
if [ -n "$backup_folders_raw" ]; then
    # iterate lines
    while IFS= read -r line; do
        line=$(echo "$line" | trim_quotes)
        [ -n "$line" ] && backup_roots+=("$line")
    done <<< "$backup_folders_raw"
fi

# If backup_roots empty -> error (we require multiple backup locations only)
if [ ${#backup_roots[@]} -eq 0 ]; then
    log_message "Error: backup_folders must be defined and contain at least one path in config.yml."
    send_email "Backup Script Failure" "Error: backup_folders must be defined and contain at least one path in config.yml."
    exit 1
fi

# Log parsed backup roots
log_message "Configured backup roots:"
for r in "${backup_roots[@]}"; do
    log_message " - $r"
done

# ---------------------------------------------------------------------
# Trim any leading or trailing quotes from other folder paths (if any)
# ---------------------------------------------------------------------
# (base_folder and log_folder already trimmed)
# No single backup_folder used anymore.

# Set the path to your data directory
data_directory="$base_folder"

# Get yesterday's date in the format YYYY-MM-DD (UTC)
yesterday=$(date -u -d "yesterday" +%F 2>/dev/null || date -u -v -1d +%F 2>/dev/null || date -u +%F)
# Extract year and month from yesterday's date
year=$(date -u -d "$yesterday" +%Y 2>/dev/null || echo "${yesterday:0:4}")
month=$(date -u -d "$yesterday" +%m 2>/dev/null || echo "${yesterday:5:2}")

# Set the input directory
input_directory="$data_directory/$year-$month/$yesterday"

# Update log_file to use actual yesterday
log_file="$log_folder/backup_log_$yesterday.txt"
touch "$log_file"

# ---------------------------------------------------------------------
# Function to generate a summary of event types
# ---------------------------------------------------------------------
generate_summary() {
    local json_file="$1"
    local summary_file="$2"
    if [ ! -s "$json_file" ]; then
        echo "Total_Count 0" > "$summary_file"
        return 0
    fi
    jq -r '.event_type' "$json_file" 2>/dev/null | sort | uniq -c | awk '{print $2, $1}' > "$summary_file"
    total_count=$(jq -s 'length' "$json_file" 2>/dev/null || echo 0)
    echo "Total_Count $total_count" >> "$summary_file"
    return 0
}

# ---------------------------------------------------------------------
# Function to clean up old log files
# ---------------------------------------------------------------------
cleanup_old_logs() {
    log_message "Starting cleanup of old log files."
    find "$log_folder" -type f -name 'backup_log_*.txt' -mtime +10 -exec rm -f {} \;
    if [ $? -eq 0 ]; then
        log_message "Old logs older than 10 days have been deleted successfully."
    else
        log_message "Warning: Failed to delete some old logs."
        # don't exit the whole script just for cleanup failure
    fi
}

# ---------------------------------------------------------------------
# Function to remove a directory with error handling
# ---------------------------------------------------------------------
remove_directory() {
    local dir="$1"
    if [ -d "$dir" ]; then
        rm -r "$dir"
        if [ $? -eq 0 ]; then
            log_message "Successfully removed directory: $dir"
        else
            log_message "Error: Failed to remove directory: $dir"
            send_email "Backup Script Failure" "Error: Failed to remove directory: $dir"
            exit 1
        fi
    else
        log_message "Warning: Directory not found (nothing to remove): $dir"
    fi
}

# ---------------------------------------------------------------------
# Helper: compute directory byte-sum for a rough integrity check
# ---------------------------------------------------------------------
dir_byte_sum() {
    local dir="$1"
    if [ ! -d "$dir" ]; then
        echo 0
        return
    fi
    # Try du -sb (Linux), fallback to find+awk
    if du -sb "$dir" &>/dev/null; then
        du -sb "$dir" | awk '{print $1}'
    else
        # find may not support -printf on all platforms; try a POSIX-safe approach
        find "$dir" -type f -exec stat -c%s {} \; 2>/dev/null | awk '{s+=$1} END{print s+0}'
    fi
}

# ---------------------------------------------------------------------
# Main: check input directory, process files, then copy to multiple backups
# ---------------------------------------------------------------------
if [ -d "$input_directory" ]; then
    # Navigate to the input directory
    cd "$input_directory" || { log_message "Error: Could not change directory to $input_directory"; send_email "Backup Script Failure" "Error: Could not change directory to $input_directory"; exit 1; }
    
    # Find all JSON files in the directory and its subdirectories
    mapfile -t json_files < <(find "$input_directory" -type f -name "*.json" -print)
    
    if [ ${#json_files[@]} -eq 0 ]; then
        log_message "No JSON files found in $input_directory"
    fi

    # Post-processing once: generate summaries (skip *_new_users.json) and gzip each JSON safely
    for file in "${json_files[@]}"; do
        # Check if the file ends with "_new_users.json" and skip summary if it does
        if [[ "$file" != *_new_users.json ]]; then
            # Generate summary for the JSON file
            summary_file="${file%.json}_summary.txt"
            generate_summary "$file" "$summary_file"
            log_message "Summary file created successfully: $summary_file"
        else
            log_message "Skipping summary generation for $file"
        fi
        
        # Safe gzip: write to tmp then move to avoid partial gz files
        gz_tmp="${file}.gz.tmp"
        gz_target="${file}.gz"
        if [ -f "$gz_target" ]; then
            log_message "Already compressed: $gz_target"
        else
            if gzip -c "$file" > "$gz_tmp"; then
                mv "$gz_tmp" "$gz_target"
                rm -f "$file"
                log_message "Gzip file created successfully: ${gz_target}"
                log_message "Original file deleted successfully: $file"
            else
                rm -f "$gz_tmp"
                log_message "Error: Failed to gzip $file"
                send_email "Backup Script Failure" "Error: Failed to gzip $file"
                exit 1
            fi
        fi
    done

    # Compress the `new_users` directory once
    new_users_dir="$input_directory/new_users"
    archive_name="new_users_$yesterday.tar.gz"
    archive_path="$input_directory/$archive_name"

    if [ -d "$new_users_dir" ]; then
        if tar -czf "$archive_path" -C "$input_directory" "new_users"; then
            log_message "Successfully created archive: $archive_path"
            remove_directory "$new_users_dir"
        else
            log_message "Error: Failed to create archive of new_users folder"
            send_email "Backup Script Failure" "Error: Failed to create archive of new_users"
            exit 1
        fi
    else
        log_message "Warning: new_users directory not found at $new_users_dir"
    fi
    
    # -----------------------------------------------------------------
    # Copy processed content to each backup root and verify
    # -----------------------------------------------------------------
    all_ok=1
    src_bytes=$(dir_byte_sum "$input_directory")
    log_message "Source byte-sum: $src_bytes"

    for root in "${backup_roots[@]}"; do
        # Trim quotes (just in case)
        root=$(echo "$root" | trim_quotes)
        target_directory="$root/$year-$month/$yesterday"
        log_message "Backing up to: $target_directory"

        # If root doesn't exist, try to create it (behaviour: attempt mkdir -p)
        if [ ! -d "$root" ]; then
            log_message "Backup root does not exist, attempting to create: $root"
            if ! mkdir -p "$root"; then
                log_message "Error: Could not create backup root: $root"
                all_ok=0
                continue
            fi
        fi

        # Ensure the target directory exists
        if [ ! -d "$target_directory" ]; then
            if ! mkdir -p "$target_directory"; then
                log_message "Error: Could not create target directory: $target_directory"
                all_ok=0
                continue
            fi
        fi

        # Copy all content to the backup location
        if cp -r "$input_directory"/* "$target_directory"; then
            log_message "Copy completed successfully to: $target_directory"
            # Verify by comparing byte-sums
            dst_bytes=$(dir_byte_sum "$target_directory")
            log_message "Target byte-sum for $root: $dst_bytes"
            if [ "$src_bytes" -eq "$dst_bytes" ] && [ "$src_bytes" -gt 0 ]; then
                log_message "Backup verified successfully for: $root"
            else
                log_message "Warning: Size mismatch for $root (src:$src_bytes dst:$dst_bytes)"
                all_ok=0
            fi
        else
            log_message "Error: Failed to copy files to backup location: $target_directory"
            send_email "Backup Script Failure" "Error: Failed to copy files to backup location: $target_directory"
            all_ok=0
        fi
    done

    # If all backups succeeded, remove the original directory; otherwise keep it
    if [ "$all_ok" -eq 1 ]; then
        log_message "All backups succeeded; removing original input directory: $input_directory"
        remove_directory "$input_directory"
    else
        log_message "One or more backups failed or mismatched; keeping local data at: $input_directory"
        send_email "Backup Script Warning" "One or more backups failed for date $yesterday. Local data retained at $input_directory for inspection."
        exit 1
    fi

else
    log_message "Error: Data directory for yesterday not found. ${input_directory}"
    send_email "Backup Script Failure" "Error: Data directory for yesterday not found. ${input_directory}"
    exit 1
fi

# Clean up old logs (non-fatal)
cleanup_old_logs
