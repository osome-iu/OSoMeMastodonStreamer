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

# Create a RotatingFileHandler
handler = RotatingFileHandler(
    filename='account_creations.log',
    maxBytes=10*1024*1024,
    backupCount=3
)

# Set the log level and format for the handler
handler.setLevel(logging.ERROR)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
handler.setFormatter(formatter)

# Configure the root logger to use the rotating handler
logging.getLogger().setLevel(logging.ERROR)
logging.getLogger().addHandler(handler)

# Helper function to load the last 80*2 URLs from an existing file
def tail(file, lines=160):
    """Return the last `lines` lines from the file, efficiently."""
    with open(file, 'rb') as f:
        buffer_size = 1024
        f.seek(0, os.SEEK_END)
        end = f.tell()
        blocks = []
        block = -1

        # Read the file backwards in chunks
        while len(blocks) < lines and f.tell() > 0:
            f.seek(max(end - buffer_size * (block + 1), 0))
            chunk = f.read(min(buffer_size, end - f.tell()))
            blocks = chunk.splitlines() + blocks
            block += 1

        # Decode the blocks properly
        decoded_lines = []
        current_json = ""
        for line in blocks:
            try:
                # Try to load each individual line as JSON
                current_json += line.decode('utf-8')
                decoded_line = json.loads(current_json)
                decoded_lines.append(decoded_line)
                current_json = ""  # Reset once we successfully decode a JSON object
            except json.JSONDecodeError:
                # If JSON decoding fails, continue concatenating lines
                continue

        return decoded_lines[-lines:]  # Return only the last `lines` decoded lines

def load_last_urls(filename, limit=80):
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
    folder_path = os.path.join(base_folder, utc_now.strftime('%Y-%m'), y_m_d_str)
    os.makedirs(folder_path, exist_ok=True)
    file_name = f"{domain}_{y_m_d_str}.json"
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

# Main function to run asynchronous tasks
async def main(servers, data_folder):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for server in servers:
            tasks.append(fetch_and_save(session, server, data_folder))
        
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

    # Run the main function with the loaded servers
    data_folder = "user_creations"
    asyncio.run(main(servers, data_folder))
