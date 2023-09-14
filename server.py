"""
Purpose:
    This script used to stream the mastodon stream API.
    This will be using only the mastodon public data.
Inputs:

Outputs:
    - JSON object
Authors: Pasan Kamburugamuwa
"""

import os, sys,json
from library import backend_util
from mastodon_streamer import streamer
import threading

# Log file location and the file
LOG_DIR = "/Users/pkamburu/IUNI/mastodon/logs"
LOG_FNAME = "mastodon_streamer_logging.log"

def run_mastodon_servers():
    script_name = os.path.basename(__file__)
    logger = backend_util.get_logger(LOG_DIR, LOG_FNAME, script_name=script_name, also_print=True)
    logger.info("-" * 50)
    logger.info(f"Begin script: {__file__}")

    mastodon_servers = os.path.abspath('library/mastodon_servers.json')

    with open(mastodon_servers, 'r') as server_file:
        mastodon_server_dict = json.load(server_file)

    # Create threads and start streaming for each instance and stream method
    threads = []
    for instance_info in mastodon_server_dict['mastodon_servers']:
        thread = threading.Thread(target=streamer.stream_public_data, args=(instance_info,))
        threads.append(thread)

    try:
        # Start all threads
        for thread in threads:
            thread.start()

        # Wait for all threads to finish
        for thread in threads:
            thread.join()

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt detected. Stopping threads...")
        for thread in threads:
            thread.join()

if __name__ == '__main__':
    script_name = os.path.basename(__file__)
    logger = backend_util.get_logger(LOG_DIR, LOG_FNAME, script_name=script_name, also_print=True)
    logger.info("-" * 50)
    logger.info(f"Begin script: {__file__}")
    run_mastodon_servers()

