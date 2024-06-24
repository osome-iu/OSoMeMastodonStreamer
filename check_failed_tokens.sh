#!/bin/bash
export TZ=UTC

# Function to read values from config.yml using yq
read_config() {
    yq "$1" config.yml
}
base_folder=$(read_config '.base_folder')
base_folder=$(echo "$base_folder" | sed 's/^"//' | sed 's/"$//')
TO=$(read_config '.backup_email')
TO=$(echo "$TO" | sed 's/^"//' | sed 's/"$//')
SUBJECT="Mastodon Streamer Lite - Failed Tokens: "

# Define file paths
log_folder=$(read_config '.log_folder')
log_folder=$(echo "$log_folder" | sed 's/^"//' | sed 's/"$//')
log_file="${log_folder}/mastodon_streamer.log"
heartbeat_file="${log_folder}/heartbeat_servers.txt"
error_file="${log_folder}/error_servers.txt"
servers_with_errors_no_heartbeat_file="${log_folder}/servers_with_errors_no_heartbeat.txt"
existing_servers_file="${log_folder}/existing_servers.txt"
filtered_servers_file="${log_folder}/filtered_servers_with_errors_no_heartbeat.txt"
formatted_errors_file="${log_folder}/formatted_errors.txt"

# Execute the command
grep "Received heartbeat" "$log_file" | awk '{ print $3 }' | sort | uniq > "$heartbeat_file"
grep "ERROR" "$log_file" | sed 's/^[^:]*:ERROR: //' | awk '{ $1=$2=""; sub(/^  */, ""); print "ERROR: " $0 }' | sort | uniq > "$error_file"
comm -23 "$error_file" "$heartbeat_file" | sed 's/https:\/\///g' > "$servers_with_errors_no_heartbeat_file"

# Set the path to your data directory
data_directory="$base_folder"
today=$(date +%F)
year=$(date +%Y)
month=$(date +%m)
today_directory="$data_directory/$year-$month/$today"

ls "$today_directory" | sed "s/_${today}\.json//" > "$existing_servers_file"

# Step 2: Filter the servers_with_errors_no_heartbeat.txt file
grep -vFf "$existing_servers_file" "$servers_with_errors_no_heartbeat_file" > "$filtered_servers_file"

# Format the output
awk -F' from |: Could not connect to streaming server: ' '{print $2 "\t\t" $3}' "$filtered_servers_file" > "$formatted_errors_file"

# Display the formatted results
cat "$formatted_errors_file"


# Check if the output file has content
if [[ -s "$formatted_errors_file" ]]; then
    # Send email notification
    echo "$MESSAGE. Sending email notification on failed tokens..."
    # Send email if there is output
    cat "$formatted_errors_file" | mailx -s "Servers with Errors but No Heartbeat or Data" "$TO"
fi

# Remove temporary files
rm "$heartbeat_file" "$error_file" "$servers_with_errors_no_heartbeat_file" "$existing_servers_file" "$filtered_servers_file" "$formatted_errors_file"