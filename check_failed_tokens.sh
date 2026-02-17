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

# NEW: failure counter file
failure_counter_file="${log_folder}/failure_counters.txt"
touch "$failure_counter_file"

# Execute the command
grep -F "Received heartbeat" "$log_file" \
  | sed -n 's/.*DEBUG:\([^ ]*\) - Received heartbeat\..*/\1/p' \
  | sort -u > "$heartbeat_file"

grep "ERROR" "$log_file" \
  | grep -vF "Failed to load server configuration from Google Sheet" \
  | sed 's/^[^:]*:ERROR: //' \
  | awk '{ $1=$2=""; sub(/^  */, ""); print "ERROR: " $0 }' \
  | sort \
  | uniq > "$error_file"

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


# Update the failure counters
declare -A counters

# Load existing counters
while read -r server count; do
    [[ -n "$server" ]] && counters["$server"]=$count
done < "$failure_counter_file"

# Read today's failing servers
today_failing=()
while read -r line; do
    server=$(echo "$line" | awk '{print $1}')
    [[ -n "$server" ]] && today_failing+=("$server")
done < "$formatted_errors_file"

# Increment counters for today's failures
for server in "${today_failing[@]}"; do
    if [[ -n "${counters[$server]}" ]]; then
        counters["$server"]=$(( counters["$server"] + 1 ))
    else
        counters["$server"]=1
    fi
done

# Reset counters for servers that recovered
for server in "${!counters[@]}"; do
    if ! printf '%s\n' "${today_failing[@]}" | grep -q "^$server$"; then
        unset counters["$server"]
    fi
done

# Save updated counters
: > "$failure_counter_file"
for server in "${!counters[@]}"; do
    echo "$server ${counters[$server]}" >> "$failure_counter_file"
done

# Send the email alert only if failing for 7+ days
alert_needed=false
alert_file="${log_folder}/servers_failing_7_days.txt"
email_body="${log_folder}/email_body.txt"

: > "$alert_file"

for server in "${!counters[@]}"; do
    if (( counters["$server"] >= 7 )); then
        alert_needed=true
        grep "^$server" "$formatted_errors_file" >> "$alert_file"
    fi
done

if $alert_needed; then
    echo "Servers failing for 7+ days" > "$email_body"
    echo "" >> "$email_body"
    cat "$alert_file" >> "$email_body"

    echo "Sending email: servers failing for 7+ days..."
    mailx -s "Servers failing for 7+ days" "$TO" < "$email_body"
fi

# Remove temporary files
rm "$heartbeat_file" "$error_file" "$servers_with_errors_no_heartbeat_file" "$existing_servers_file" "$filtered_servers_file" "$formatted_errors_file"
