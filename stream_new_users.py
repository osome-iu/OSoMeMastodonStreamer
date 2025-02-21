"""
Purpose:
    Stream and save new user data from Mastodon servers.
    Mastodon Directory Ref: https://docs.joinmastodon.org/methods/directory/
    Google Service Account Ref: https://cloud.google.com/iam/docs/service-accounts-create
    Google Sheet Python Ref: https://developers.google.com/sheets/api/quickstart/python

Inputs:
    'config.yml' - copy and start with 'config.yml.template'
    
    json file with list of server names and tokens -> {"mastodon_servers":[{"access_token(not needed)":"","api_base_url":""},...,...,...]}
        or
    Google Sheet derived from https://instances.social/

Outputs:
    individual files for each instance, in path f"{config['base_folder']}/yyyy-mm/yyyy-mm-dd/new_users/{instance_name}_yyyy-mm-dd.json"

    
Author(s): Nick Liu
"""
import aiohttp
import asyncio
import json
import os
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone
from google.oauth2.service_account import Credentials
import yaml
import gspread
import sys
import time
from collections import deque

# Load configuration from config.yml
with open('config.yml', 'r') as config_file:
    config = yaml.safe_load(config_file)

# Create log directory if not exists
log_folder = config['log_folder']
if not os.path.exists(log_folder):
    os.makedirs(log_folder)

# Configure logging
log_file_path = os.path.join(log_folder, 'new_users.log')

# Set up a rotating file handler to keep logs within a 10 MB limit
log_handler = RotatingFileHandler(log_file_path, maxBytes=10*1024*1024, backupCount=2)
log_handler.setLevel(logging.DEBUG)
log_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s:%(message)s'))

# Apply the handler to the root logger
logging.getLogger().addHandler(log_handler)
logging.getLogger().setLevel(logging.DEBUG)

def tail(file, lines=160):
    """Return the last `lines` lines from the file using a sliding window (deque)."""
    sliding_window = deque(maxlen=lines)  # A deque to hold the last `lines` lines
    
    with open(file, 'r', encoding='utf-8') as f:
        for line in f:
            sliding_window.append(line)  # Append each line, deque will automatically discard the oldest

    # Decode the sliding window's contents as JSON
    decoded_lines = []
    current_json = ""
    for line in sliding_window:
        current_json += line
        try:
            decoded_line = json.loads(current_json)  # Try decoding the accumulated JSON string
            decoded_lines.append(decoded_line)
            current_json = ""  # Reset after successful decoding
        except json.JSONDecodeError:
            # If incomplete JSON, continue accumulating lines
            continue

    return decoded_lines

def load_last_urls(filename, limit=160):
    """Loads the last `limit` number of URLs from the file."""
    last_urls = set()
    if os.path.exists(filename):
        try:
            json_data = tail(filename, limit)
            for user_data in json_data:
                user_url = user_data.get("url")
                if user_url:
                    last_urls.add(user_url)
        except Exception as e:
            logging.error(f"Error reading file {filename}: {e}")
    logging.info(f"Loaded {len(last_urls)} latest URLs from file.")
    return last_urls

# Create folder structure based on the current UTC date
def get_file_path(base_folder, domain):
    utc_now = datetime.now(timezone.utc)
    y_m_d_str = utc_now.strftime('%Y-%m-%d')
    folder_path = os.path.join(base_folder, utc_now.strftime('%Y-%m'), y_m_d_str, "new_users")
    os.makedirs(folder_path, exist_ok=True)
    file_name = f"{domain}_{y_m_d_str}_new_users.json"
    file_path = os.path.join(folder_path, file_name)

    collected_at = utc_now.timestamp()
    collected_at_str = utc_now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return file_path, collected_at, collected_at_str

# Asynchronous function to fetch and save data for a domain
async def fetch_and_save(session, server_info, base_folder):
    domain = server_info
    url = f"https://{domain}/api/v1/directory"
    params = {'order': 'new', 'limit': 80}
    headers = {
        'User-Agent': 'curl/7.68.0',  # Mimic curl's User-Agent
        }

    # Get the file path for storing the data
    file_path, _, _ = get_file_path(base_folder, domain)

    # Load last URLs if the file exists
    last_urls = load_last_urls(file_path)

    while True:
        try:
            start_time = time.time()  # Track the start time

            async with session.get(url, params=params, headers=headers) as response:
                if response.status != 200:
                    logging.error(f"Failed to fetch data from {domain}: {response.status}")
                    return
                
                data = await response.json()

                # Get the file path for storing the data
                file_path, collected_at, collected_at_str = get_file_path(base_folder, domain)

                # Filter new users by checking if their URL is not in last_urls
                new_users = [user for user in data if user.get("url") and user.get("url") not in last_urls]

                # Log how many new users were found
                logging.info(f"Domain: {domain}, New users received: {len(new_users)}")

                if new_users:
                    new_records = []
                    base_record = {
                        'collected_at': collected_at,
                        'collected_at_str': collected_at_str
                    }

                    # Accumulate all the new users in the list
                    for user in new_users:
                        new_record = base_record.copy()
                        new_record.update(user)
                        new_records.append(new_record)

                    # Write all new users to the file at once
                    with open(file_path, "a") as f:
                        for record in new_records:
                            f.write(json.dumps(record) + "\n")

                    # Update last_urls with the new URLs we just processed
                    last_urls.update(user.get("url") for user in new_users)

            # Calculate elapsed time for request and processing
            elapsed_time = time.time() - start_time
            sleep_time = max(0, 1 - elapsed_time)  # Sleep only the remaining time if any
            await asyncio.sleep(sleep_time)

        except Exception as e:
            logging.error(f"Failed to fetch data from {domain}: {e}")



# Function to load server configurations from Google Sheets
def load_servers_from_google_sheet(sheet_id, sheet_name, credentials_json):
    try:
        # Define the scope and create credentials
        scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
        credentials = Credentials.from_service_account_file(credentials_json, scopes=scopes)

        # Authorize and get the sheet
        client = gspread.authorize(credentials)
        sheet = client.open_by_key(sheet_id).worksheet(sheet_name)
        
        # Get all records from the sheet
        records = sheet.get_all_records()

        # Deduplicate servers
        unique_servers = set()
        
        for server in records:
            domain = server['name']
            unique_servers.add(domain)
        
        logging.info(f"Loaded server configurations for {len(unique_servers)} servers.")
        return unique_servers
    
    except Exception as e:
        logging.error(f"Failed to load server configuration from Google Sheet: {e}")
        sys.exit(1)

def load_servers_from_json(json_file):
    if not os.path.exists(json_file):
        logging.error(f"Server configuration file {json_file} does not exist.")
        sys.exit(1)
        
    try:
        with open(json_file, 'r') as f:
            data = json.load(f)
            server_list = data['mastodon_servers']
            
            # Deduplicate servers
            unique_servers = set()
            
            for server in server_list:
                domain = server['api_base_url'].split("/")[-1]
                unique_servers.add(domain)
            
            logging.info(f'Configs loaded for {len(unique_servers)} unique server(s).')
        return unique_servers
    except Exception as e:
        logging.error(f"Failed to load server configuration from {json_file}: {e}")
        sys.exit(1)


# Main function to run asynchronous tasks
async def main(servers, base_folder):
    logging.info(f"Saving files to {base_folder} ...")
    async with aiohttp.ClientSession() as session:
        tasks = []
        for server in servers:
            tasks.append(fetch_and_save(session, server, base_folder))
        
        # Run all tasks concurrently
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    # Load configuration from config.yml
    with open('config.yml', 'r') as config_file:
        config = yaml.safe_load(config_file)

    # Google Sheets settings
    GOOGLE_SHEET_ID = config['google_sheet_id']
    RANGE_NAME = config['range_name']
    CREDENTIALS_JSON = config['credentials_json']

    # Load servers from Google Sheets
    servers = load_servers_from_google_sheet(GOOGLE_SHEET_ID, RANGE_NAME, CREDENTIALS_JSON)

    # # Load servers from JSON file
    # servers = load_servers_from_json(config['server_list_json'])

    # Run the main function with the loaded servers
    # Create derived data folder if not exists
    base_folder = config['base_folder']
    if not os.path.exists(base_folder):
        os.makedirs(base_folder)
    asyncio.run(main(servers, base_folder))
