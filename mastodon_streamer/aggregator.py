"""
Purpose:
    This script used to make the aggregate streamer for the mastodon data.
Inputs:
    - functions
        -

Outputs:
    - JSON object
Note: For the version 1, Get the data from two instances, mastodon.social and mastodon.cloud.
Authors: Pasan Kamburugamuwa
"""

import datetime, gzip, os, json
from mastodon import Mastodon, StreamListener
from library import backend_util

# Specify the directory path where the files will be stored
DATA_DERIVED_DIR = "/Users/pkamburu/IUNI/mastodon/data_derived/aggregator"
LOG_DIR = "/Users/pkamburu/IUNI/mastodon/logs"

# Create a logger
LOG_FNAME = "mastodon_streamer_logging.log"
script_name = os.path.basename(__file__)
logger = backend_util.get_logger(LOG_DIR, LOG_FNAME, script_name=script_name, also_print=True)

class MastodonStreamAggregator(StreamListener):

    def __init__(self, instance_name):
        super().__init__()

        self.instance_name = instance_name

        self.current_hour_posts = 0
        self.posts_count_per_day = 0

        # Get the current date
        self.current_date = datetime.datetime.now().date()
        # Get the current hour
        self.current_hour = datetime.datetime.now().hour
        self.file_name = self.get_file_name()

    #Get the file name which is
    def get_file_name(self):
        try:
            # Get the current month and date for the filename
            current_month = datetime.datetime.now().strftime("%Y-%m")
            current_date = datetime.datetime.now().strftime("%Y-%m-%d")

            logger.info(f"Get the combined file - {self.file_name}")
            return os.path.join(DATA_DERIVED_DIR, current_month, "pasan", f"{current_date}", f"{current_date}.json")

        except Exception as e:
            logger.error(f"An error occurred: {str(e)}")


    def on_update(self, status):
        """
        Increment the post count for each received toot
        Parameters
        -----------
        None
        Returns
        -----------
        .gz file create and remove the mastodon_{current_date}.json file.
        """
        try:
            # Increment the post count for each received toot
            self.posts_count_per_day += 1
            self.current_hour_posts += 1
        except Exception as e:
            logger.error(f"An error occured while incrementing post counts: {str(e)}")


        try:
            # In this function is used to check if there is a new date started.
            now = datetime.datetime.now()
            if now.date() != self.current_date:
                self.end_of_day()
        except Exception as e:
            logger.error(f"An error occured while checking for a new date: {str(e)}")

        try:
            # Check if an hour has passed, print the result and reset the counter
            current_hour = datetime.datetime.now().hour
            if current_hour != self.current_hour:
                self.log_posts_and_file_size()
                self.current_hour_posts = 1
                self.current_hour = current_hour
        except Exception as e:
            logger.error(f"An error occurred while checking for a new hour and logging: {str(e)}")

        # Write toot info to JSON file
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
        }

        # Create directories for the current month and date if they don't exist
        os.makedirs(os.path.dirname(self.file_name), exist_ok=True)

        try:
            os.makedirs(os.path.dirname(self.file_name), exist_ok=True)
            with open(self.file_name, 'a') as file:
                json.dump(toot_info, file, default=str)
                file.write('\n')
        except FileNotFoundError as e:
            logger.error(f"File not found: {str(e)}")
        except IOError as e:
            logger.error(f"IO error while writing to the file: {str(e)}")
        except Exception as e:
            logger.error(f"An error occurred while writing to the file: {str(e)}")


#this used to aggregate all the two instance at once.
def stream_aggregate_public_data(instance_info):

    # Create a Mastodon client
    mastodon_stream = Mastodon(
        access_token=instance_info['access_token'],
        api_base_url=instance_info['api_base_url']
    )

    # Use the access token for post streaming
    stream_listener = MastodonStreamAggregator(instance_info['api_base_url'])
    mastodon_stream.stream_public(stream_listener)
