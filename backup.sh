#!/bin/bash
export TZ=UTC

# Function to read values from config.yml using yq
read_config() {
    yq "$1" config.yml
}

# Function to send an email using mailx
send_email() {
    local subject="$1"
    local message="$2"
    echo "$message" | mailx -s "$subject" "$backup_email"
}

# Function to log messages with a timestamp
log_message() {
    local message="$1"
    local timestamp=$(date +"%Y-%m-%d %H:%M:%S")
    echo "[$timestamp] $message" | tee -a "$log_file"
}

# Read base_folder, log_folder, backup_email from config.yml
base_folder=$(read_config '.base_folder')
log_folder=$(read_config '.log_folder')
backup_email=$(read_config '.backup_email')

### Read multiple backup folders
backup_folders=($(read_config '.backup_folders[]'))

# Check if yq was able to read the config values
if [ -z "$base_folder" ]; then
    log_message "Error: Could not read base_folder from config.yml."
    send_email "Backup Script Failure" "Error: Could not read base_folder from config.yml."
    exit 1
fi

if [ ${#backup_folders[@]} -eq 0 ]; then
    log_message "Error: Could not read backup_folders from config.yml."
    send_email "Backup Script Failure" "Error: Could not read backup_folders from config.yml."
    exit 1
fi

if [ -z "$log_folder" ]; then
    log_message "Error: Could not read log_folder from config.yml."
    send_email "Backup Script Failure" "Error: Could not read log_folder from config.yml."
    exit 1
fi

if [ -z "$backup_email" ]; then
    log_message "Error: Could not read backup_email from config.yml."
    send_email "Backup Script Failure" "Error: Could not read backup_email from config.yml."
    exit 1
fi

# Trim any leading or trailing quotes
base_folder=$(echo "$base_folder" | sed 's/^"//' | sed 's/"$//')
log_folder=$(echo "$log_folder" | sed 's/^"//' | sed 's/"$//')
backup_email=$(echo "$backup_email" | sed 's/^"//' | sed 's/"$//')

# Trim quotes for each backup folder
clean_backup_folders=()
for folder in "${backup_folders[@]}"; do
    clean_backup_folders+=("$(echo "$folder" | sed 's/^"//' | sed 's/"$//')")
done
backup_folders=("${clean_backup_folders[@]}")

# Ensure the log_folder exists
mkdir -p "$log_folder"

# Set the path to your data directory
data_directory="$base_folder"

# Get yesterday's date in the format YYYY-MM-DD
yesterday=$(date -d "yesterday" +%F)
year=$(date -d "$yesterday" +%Y)
month=$(date -d "$yesterday" +%m)

# Set the input directory
input_directory="$data_directory/$year-$month/$yesterday"

# Log file
log_file="$log_folder/backup_log_$yesterday.txt"

# Function to generate a summary of event types
generate_summary() {
    local json_file="$1"
    local summary_file="$2"
    jq -r '.event_type' "$json_file" | sort | uniq -c | awk '{print $2, $1}' > "$summary_file"
    total_count=$(jq -s 'length' "$json_file")
    echo "Total_Count $total_count" >> "$summary_file"
}

# Function to clean up old log files
cleanup_old_logs() {
    log_message "Starting cleanup of old log files."
    find "$log_folder" -type f -name 'backup_log_*.txt' -mtime +10 -exec rm {} \;
    if [ $? -eq 0 ]; then
        log_message "Old logs older than 10 days have been deleted successfully."
    else
        log_message "Error: Failed to delete old logs."
        send_email "Backup Script Failure" "Error: Failed to delete old logs from $log_folder."
        exit 1
    fi
}

# Function to remove a directory with error handling
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
        log_message "Error: Directory not found: $dir"
        send_email "Backup Script Failure" "Error: Directory not found: $dir"
        exit 1
    fi
}

# Check if the input directory exists
if [ -d "$input_directory" ]; then
    cd "$input_directory" || { log_message "Error: Could not change directory to $input_directory"; send_email "Backup Script Failure" "Error: Could not change directory to $input_directory"; exit 1; }

    find "$input_directory" -type f -name "*.json" | while read -r file; do
        
        if [[ "$file" != *_new_users.json ]]; then
            summary_file="${file%.json}_summary.txt"
            generate_summary "$file" "$summary_file"
            log_message "Summary file created successfully: $summary_file"
        else
            log_message "Skipping summary generation for $file"
        fi
        
        gzip -c "$file" > "${file}.gz"
        if [ $? -eq 0 ]; then
            rm "$file"
            log_message "Gzip file created successfully: ${file}.gz"
            log_message "Original file deleted successfully: $file"
        else
            log_message "Error: Failed to gzip $file"
            send_email "Backup Script Failure" "Error: Failed to gzip $file"
            exit 1
        fi
    done

    new_users_dir="$input_directory/new_users"
    archive_name="new_users_$yesterday.tar.gz"
    archive_path="$input_directory/$archive_name"

    if [ -d "$new_users_dir" ]; then
        tar -czf "$archive_path" -C "$input_directory" "new_users"
        if [ $? -eq 0 ]; then
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

    # Create a .tar of the date folder (e.g. 2026-10-12.tar)
    # Built next to the date folder so the tar doesn't include itself
    tar_file="$data_directory/$year-$month/$yesterday.tar"
    tar -cf "$tar_file" -C "$data_directory/$year-$month" "$yesterday"
    if [ $? -eq 0 ]; then
        log_message "Successfully created tar file: $tar_file"
    else
        log_message "Error: Failed to create tar file: $tar_file"
        send_email "Backup Script Failure" "Error: Failed to create tar file: $tar_file"
        rm -f "$tar_file"
        exit 1
    fi

    # Multiple backup locations (now copies the .tar instead of the folder)
    for backup_folder in "${backup_folders[@]}"; do
        target_directory="$backup_folder/$year-$month"

        mkdir -p "$target_directory"
        if [ $? -ne 0 ]; then
            log_message "Error: Failed to create backup directory: $target_directory"
            send_email "Backup Script Failure" "Error: Failed to create backup directory: $target_directory"
            exit 1
        fi

        cp "$tar_file" "$target_directory/"
        if [ $? -eq 0 ]; then
            log_message "Backup completed successfully to: $target_directory/$yesterday.tar"
        else
            log_message "Error: Failed to copy tar to backup location: $target_directory"
            send_email "Backup Script Failure" "Error: Failed to copy tar to backup location: $target_directory"
            exit 1
        fi
    done

    # Only remove original directory and temporary tar after ALL backups succeed
    remove_directory "$input_directory"
    rm -f "$tar_file"

    # Only remove original directory after ALL backups succeed
    remove_directory "$input_directory"

else
    log_message "Error: Data directory for $yesterday not found. ${input_directory}"
    send_email "Backup Script Failure" "Error: Data directory for $yesterday not found. ${input_directory}"
    exit 1
fi

cleanup_old_logs
