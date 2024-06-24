"""
Purpose:
    Stream and save data from Mastodon servers given server name and access token.
    Mastodon Streaming Ref: https://docs.joinmastodon.org/methods/streaming/
    Google Service Account Ref: https://cloud.google.com/iam/docs/service-accounts-create
    Google Sheet Python Ref: https://developers.google.com/sheets/api/quickstart/python

Inputs:
    'config.yml' - copy and start with 'config.yml.template'
    
    json file with list of server names and tokens -> {"mastodon_servers":[{"access_token":"","api_base_url":""},...,...,...]}
        or
    Google Sheet derived from https://instances.social/

Outputs:
    individual files for each instance, in path f"{config['base_folder']}/yyyy-mm/yyyy-mm-dd/{instance_name}_yyyy-mm-dd.json"

Author(s): Nick Liu
"""

import os
import sys
import json
from datetime import datetime, timezone
import logging
from time import sleep
from mastodon import Mastodon, StreamListener
from urllib.parse import urlparse
import threading
import gspread
# from google.oauth2.credentials import Credentials
from google.oauth2.service_account import Credentials
import yaml

# Load configuration from config.yml
with open('config.yml', 'r') as config_file:
    config = yaml.safe_load(config_file)

# Create log directory if not exists
log_folder = config['log_folder']
if not os.path.exists(log_folder):
    os.makedirs(log_folder)

# Configure logging
log_file_path = os.path.join(log_folder, 'mastodon_streamer.log')
logging.basicConfig(filename=log_file_path, level=logging.DEBUG, 
                    format='%(asctime)s %(levelname)s:%(message)s')

# Create derived data folder if not exists
base_folder = config['base_folder']
if not os.path.exists(base_folder):
    os.makedirs(base_folder)

class MyStreamListener(StreamListener):
    def __init__(self, server):
        self.server = self.sanitize_server_url(server)

    @staticmethod
    def sanitize_server_url(url):
        return urlparse(url).netloc

    def on_update(self, status):
        self.save_event('update', status)

    def on_delete(self, status_id):
        self.save_event('delete', {"id" : status_id})

    def on_status_update(self, status):
        self.save_event('status.update', status)

    def save_event(self, event_type, event_data):
        utc_now = datetime.now(timezone.utc)
        folder_path = os.path.join(base_folder, utc_now.strftime('%Y-%m'), utc_now.strftime('%Y-%m-%d'))
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        
        file_name = f"{self.server}_{utc_now.strftime('%Y-%m-%d')}.json"
        file_path = os.path.join(folder_path, file_name)
        
        event = {'event_type': event_type, 'system_date': utc_now.strftime('%Y-%m-%dT%H:%M:%SZ')}
        event.update(event_data)

        try:
            with open(file_path, 'a') as f:
                f.write(json.dumps(event, default=str) + '\n')
            # logging.info(f"Event saved to {file_path}")
        except Exception as e:
            logging.error(f"{self.server} - Failed to save event: {e}")

    def handle_heartbeat(self):
        logging.debug(f'{self.server} - Received heartbeat.')

    def on_abort(self, err):
        logging.error(f'{self.server} - Streaming aborted:{err}')

def load_servers_from_json(json_file):
    if not os.path.exists(json_file):
        logging.error(f"Server configuration file {json_file} does not exist.")
        sys.exit(1)
        
    try:
        with open(json_file, 'r') as f:
            data = json.load(f)
            server_list = data['mastodon_servers']
            
            # Deduplicate servers
            unique_servers = {}
            duplicates = []
            
            for server in server_list:
                api_base_url = server['api_base_url']
                if api_base_url in unique_servers:
                    duplicates.append(server)
                else:
                    unique_servers[api_base_url] = server['access_token']
            
            if duplicates:
                logging.warning(f"Found {len(duplicates)} duplicate server(s):")
                for dup in duplicates:
                    logging.warning(f"Duplicate: {dup['api_base_url']} with access token {dup['access_token']}")
            
            # Convert unique_servers back to list format
            deduplicated_servers = [{'api_base_url': k, 'access_token': v} for k, v in unique_servers.items()]
            logging.info(f'Configs loaded for {len(deduplicated_servers)} unique server(s).')
        return deduplicated_servers
    except Exception as e:
        logging.error(f"Failed to load server configuration from {json_file}: {e}")
        sys.exit(1)

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

        # Filter records that can stream data
        filtered_records = [record for record in records if record.get("Can stream data? (Yes, or specify reason y not)") == "Yes"]

        # Log the number of server configurations loaded
        logging.info(f'Configs loaded for {len(filtered_records)} server(s) that can stream data.')
        
        # Deduplicate servers
        unique_servers = {}
        duplicates = []
        
        for server in filtered_records:
            api_base_url = f"https://{server['name']}"
            access_token = server['Access Token']
            if api_base_url in unique_servers:
                if unique_servers[api_base_url] != access_token:
                    duplicates.append(server)
            else:
                unique_servers[api_base_url] = access_token
        
        if duplicates:
            logging.warning(f"Found {len(duplicates)} duplicate server(s) with different access tokens:")
            for dup in duplicates:
                logging.warning(f"Duplicate: https://{dup['name']} with access token {dup['Access Token']}")
        
        # Convert unique_servers back to list format
        deduplicated_servers = [{'api_base_url': k, 'access_token': v} for k, v in unique_servers.items()]
        
        logging.info(f"Loaded server configurations for {len(deduplicated_servers)} servers.")
        return deduplicated_servers
    except Exception as e:
        logging.error(f"Failed to load server configuration from Google Sheet: {e}")
        sys.exit(1)


def start_streaming(server_info):
    while True:
        try:
            mastodon = Mastodon(
                access_token=server_info['access_token'],
                api_base_url=server_info['api_base_url']
            )
            
            listener = MyStreamListener(server_info['api_base_url'])
            logging.info(f"Listening to {server_info['api_base_url']}")
            # Stream federated public timeline
            mastodon.stream_public(listener)
            
            # # Stream local public timeline
            # mastodon.stream_local(listener)
        except Exception as e:
            logging.error(f"Error in streaming from {server_info['api_base_url']}: {e}")
            sleep(10)  # Wait for 10 seconds before trying to reconnect

if __name__ == "__main__":
    # Google Sheets settings
    GOOGLE_SHEET_ID = config['google_sheet_id']
    RANGE_NAME = config['range_name']
    CREDENTIALS_JSON = config['credentials_json']

    # Load servers from Google Sheets
    servers = load_servers_from_google_sheet(GOOGLE_SHEET_ID, RANGE_NAME, CREDENTIALS_JSON)

    # # Load servers from JSON file
    # servers = load_servers_from_json(config['server_list_json'])

    threads = []
    for server_info in servers:
        thread = threading.Thread(target=start_streaming, args=(server_info,))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()
