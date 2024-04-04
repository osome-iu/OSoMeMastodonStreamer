"""
Purpose:
    This script used to stream the mastodon stream API.
    This is using the mastodonpy package to retrieve federeated data.
    Link - https://mastodonpy.readthedocs.io/en/stable/10_streaming.html#streamlistener
Inputs:

Outputs:
    - JSON object
Authors: Pasan Kamburugamuwa
"""

import os,json
from library import backend_util
from app import streamer
import threading


# Create a logger for this file.
script_name = os.path.basename(__file__)
logger = backend_util.get_logger(backend_util.LOG_DIR, backend_util.LOG_FNAME, script_name=script_name, also_print=True)

def run_mastodon_servers():

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

    except (KeyboardInterrupt, Exception) as e:
        logger.exception("Exception detected: {}".format(e))
        logger.info("Stopping threads...")
        for thread in threads:
            thread.join()



if __name__ == '__main__':
    script_name = os.path.basename(__file__)
    logger = backend_util.get_logger(backend_util.LOG_DIR, backend_util.LOG_FNAME, script_name=script_name, also_print=True)
    logger.info("-" * 50)
    logger.info(f"Begin script: {__file__}")
    logger.info(f"Start streaming mastodon data : {__file__}")
    run_mastodon_servers()



