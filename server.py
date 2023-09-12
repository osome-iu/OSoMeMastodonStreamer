"""
Purpose:
    This script used to stream the mastodon stream API
Inputs:

Outputs:
    - JSON object
Authors: Pasan Kamburugamuwa
"""

import os, sys
from library import backend_util
from mastodon import Mastodon, StreamListener
from mastodon_streamer import streamer

# Log file location and the file
LOG_DIR = "/Users/pkamburu/IUNI/mastodon/logs"
LOG_FNAME = "mastodon_streamer_logging.log"


# Create a Mastodon client instance
mastodon = Mastodon(
    access_token='LDqotkJzIDexTZ56ASuz23kO50YmMYhI5Ojt2cuapcE',
    api_base_url='https://mastodon.social'
)

if __name__ == '__main__':
    script_name = os.path.basename(__file__)
    logger = backend_util.get_logger(LOG_DIR, LOG_FNAME, script_name=script_name, also_print=True)
    logger.info("-" * 50)

    listener = streamer.MastodonStreamListener();
    mastodon.stream_public(listener)

    logger.info(f"Begin script: {__file__}")


