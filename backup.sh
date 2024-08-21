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

# Read base_folder, backup_folder, log_folder, and backup_email from config.yml
base_folder=$(read_config '.base_folder')
backup_folder=$(read_config '.backup_folder')
log_folder=$(read_config '.log_folder')
backup_email=$(read_config '.backup_email')

# Check if yq was able to read the config values
if [ -z "$base_folder" ]; then
    log_message "Error: Could not read base_folder from config.yml."
    send_email "Backup Script Failure" "Error: Could not read base_folder from config.yml."
    exit 1
fi

if [ -z "$backup_folder" ]; then
    log_message "Error: Could not read backup_folder from config.yml."
    send_email "Backup Script Failure" "Error: Could not read backup_folder from config.yml."
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

# Trim any leading or trailing quotes from the folder paths
base_folder=$(echo "$base_folder" | sed 's/^"//' | sed 's/"$//')
backup_folder=$(echo "$backup_folder" | sed 's/^"//' | sed 's/"$//')
log_folder=$(echo "$log_folder" | sed 's/^"//' | sed 's/"$//')
backup_email=$(echo "$backup_email" | sed 's/^"//' | sed 's/"$//')

# Ensure the log_folder exists
mkdir -p "$log_folder"

# Set the path to your data directory
data_directory="$base_folder"

# Get yesterday's date in the format YYYY-MM-DD
yesterday=$(date -d "yesterday" +%F)
# Extract year and month from yesterday's date
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

# Check if the input directory exists
if [ -d "$input_directory" ]; then
    # Navigate to the input directory
    cd "$input_directory" || { log_message "Error: Could not change directory to $input_directory"; send_email "Backup Script Failure" "Error: Could not change directory to $input_directory"; exit 1; }
    # Check if there are any JSON files to process
    if ls *.json 1> /dev/null 2>&1; then
        # Create gzip files for each individual file and delete the original JSON files
        for file in *.json; do
            # Generate summary for the JSON file
            summary_file="${file%.json}_summary.txt"
            generate_summary "$file" "$summary_file"
            
            # Gzip the JSON file
            gzip -c "$file" > "${file}.gz"
            if [ $? -eq 0 ]; then
                rm "$file"
                log_message "Gzip file created successfully: ${file}.gz"
                log_message "Original file deleted successfully: $file"
                log_message "Summary file created successfully: $summary_file"
            else
                log_message "Error: Failed to gzip $file"
                send_email "Backup Script Failure" "Error: Failed to gzip $file"
                exit 1
            fi
        done
        # Create the target directory in the backup location if it doesn't exist
        target_directory="$backup_folder/$year-$month/$yesterday"
        if [ ! -d "$target_directory" ]; then
            mkdir -p "$target_directory"
        fi
        # Copy all content to the backup location
        cp -r "$input_directory"/* "$target_directory"
        if [ $? -eq 0 ]; then
            log_message "Backup completed successfully to: $target_directory"
        else
            log_message "Error: Failed to copy files to backup location: $target_directory"
            send_email "Backup Script Failure" "Error: Failed to copy files to backup location: $target_directory"
            exit 1
        fi
    else
        log_message "No JSON files found to process in $input_directory"
        send_email "Backup Script Failure" "No JSON files found to process in $input_directory"
        exit 1
    fi
else
    log_message "Error: Data directory for yesterday not found. ${input_directory}"
    send_email "Backup Script Failure" "Error: Data directory for yesterday not found. ${input_directory}"
    exit 1
fi
