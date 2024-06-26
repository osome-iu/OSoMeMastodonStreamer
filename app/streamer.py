"""
Purpose:
    This script used to get the app api and data. post data.
Inputs:
    - functions
        - get_file_name (get the file name)
        - on_update(call the get data function)
        - print_posts_and_file_size(print post and file size)

Outputs:
    - JSON object
Authors: Pasan Kamburugamuwa
"""

import gzip, os, json,shutil, tarfile
from mastodon import Mastodon, StreamListener
from library import backend_util
import sys
import schedule
import time
from datetime import datetime, timedelta, timezone

# Create a logger for this file.
script_name = os.path.basename(__file__)
logger = backend_util.get_logger(backend_util.LOG_DIR, backend_util.LOG_FNAME, script_name=script_name, also_print=True)


# Define a custom stream listener
class MastodonStreamListener(StreamListener):
    def __init__(self, instance_name):
        super().__init__()

        self.instance_name = instance_name
        self.system_date, self.file_name = self.get_exact_file_name()

    def get_exact_file_name(self):
        """
        This function is used to get the exact files names to store the streaming data.
        Parameters
            None
        Returns
            File paths referring to different mastodon servers.
        -----------
        """
        try:
            # Get the current time in UTC
            current_time_utc = datetime.now(timezone.utc)
            current_year_month = current_time_utc.strftime("%Y-%m")
            current_date = current_time_utc.strftime("%Y-%m-%d")

            instance_name = self.instance_name[len("https://"):]
            file_path = os.path.join(backend_util.DATA_DERIVED_DIR, current_year_month, f"{current_date}", f"{instance_name}_{current_date}.json")
            return current_time_utc, file_path
        except Exception as e:
            logger.error(f"An error occurred: {str(e)}")

    def on_update(self, status):
        """
        Collect the mastodon streaming data from the following format and
        if the date is changed, then save the data to a new file.
        Parameters
        -----------
        None
        Returns
        -----------
        .gz file create and remove the mastodon_{current_date}.json file.
        """
        # Write toot info to JSON file with this format.
        toot_info = {
            'id': status['id'],  # ID of the status in the database.
            'uri': status['uri'],  # URI of the status used for federation
            'created_at': status['created_at'],  # The date when this status was created
            'account': status['account'],  # The account that authored this status.
            'content': status['content'],  # HTML-encoded status content.
            'visibility': status['visibility'],  # Toot visibility ('public', 'unlisted', 'private', or 'direct')
            'sensitive': status['sensitive'],  # Is this status marked as sensitive content?
            'spoiler_text': status['spoiler_text'],# Subject or summary line, below which status content is collapsed until expanded.
            'media_attachments': status['media_attachments'],  # Media that is attached to this status.
            'mentions': status['mentions'],  # Mentions of users within the status content.
            'tags': status['tags'],  # Hashtags used within the status content.
            'emojis': status['emojis'],  # Custom emoji to be used when rendering status content.
            'favourites_count': status['favourites_count'],  # How many favourites this status has received.
            'replies_count': status['replies_count'],  # How many replies this status has received.
            'url': status['url'],  # A link to the status’s HTML representation.
            'in_reply_to_id': status['in_reply_to_id'],  # ID of the status being replied to.
            'in_reply_to_account_id': status['in_reply_to_account_id'], # ID of the account that authored the status being replied to.
            'reblog': status['reblog'],  # The status being reblogged.
            'poll': status['poll'],  # The poll attached to the status.
            'card': status['card'],  # Preview card for links included within status content.
            'language': status['language'],  # Primary language of this status.
            'edited_at': status['edited_at'],  # Timestamp of when the status was last edited.
            'system_date': self.system_date, # Write the system date
            'favourited': status.get('favourited'),  # Optional field - If the current token has an authorized user
            'reblogged': status.get('reblogged'),  # Optional field - If the current token has an authorized user
            'muted': status.get('muted'),  # Optional field - If the current token has an authorized user
            'bookmarked': status.get('bookmarked'),  # Optional field - If the current token has an authorized user
            'pinned': status.get('pinned'),  # Optional field - If the current token has an authorized user
            'filtered': status.get('filtered'),  # Optional field - If the current token has an authorized user
            'text': status.get('text'),  # Optional field -
        }

        # Create directories for the current month and date if they don't exist
        os.makedirs(os.path.dirname(self.file_name), exist_ok=True)

        # write the data to the file.
        with open(self.file_name, 'a+') as file:
            json.dump(toot_info, file, default=str)
            file.write('\n')

def stream_public_data(instance_info):
    while True:
        try:
            # Create a Mastodon client
            mastodon_stream = Mastodon(
                access_token=instance_info['access_token'],
                api_base_url=instance_info['api_base_url']
            )
            # Use the access token for post streaming
            stream_listener = MastodonStreamListener(instance_info['api_base_url'])
            mastodon_stream.stream_public(stream_listener)
        except ConnectionError as e:
            logger.error(f"A connection error occurred while streaming public data: {str(e)}")
            time.sleep(60) #if the server cease communication, retry after 60 seconds.
        except Exception as e:
            logger.error(
                f"An unexpected error occurred while streaming public data: {str(e)} - {instance_info['api_base_url']}")
            time.sleep(60) #if the server cease communication, retry after 60 seconds.


