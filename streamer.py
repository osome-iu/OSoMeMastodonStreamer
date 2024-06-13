import os
import json
from datetime import datetime, timezone
import logging
from time import sleep
from mastodon import Mastodon, StreamListener
from urllib.parse import urlparse
import threading


# Create log directory if not exists
log_folder = "mastodon_log"
if not os.path.exists(log_folder):
    os.makedirs(log_folder)

# Configure logging
log_file_path = os.path.join(log_folder, 'mastodon_streamer.log')
logging.basicConfig(filename=log_file_path, level=logging.INFO, 
                    format='%(asctime)s %(levelname)s:%(message)s')

# Create derived data folder if not exists
base_folder = "mastodon_derived_data"
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

def load_servers(json_file):
    with open(json_file, 'r') as f:
        data = json.load(f)
    return data['mastodon_servers']

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
    # Load servers from JSON file
    servers = load_servers('library/mastodon_servers.json')

    threads = []
    for server_info in servers:
        thread = threading.Thread(target=start_streaming, args=(server_info,))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()
