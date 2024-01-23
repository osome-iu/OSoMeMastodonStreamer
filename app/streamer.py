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

import datetime, gzip, os, json,shutil, tarfile
from mastodon import Mastodon, StreamListener
from library import backend_util

# Create a logger for this file.
script_name = os.path.basename(__file__)
logger = backend_util.get_logger(backend_util.LOG_DIR, backend_util.LOG_FNAME, script_name=script_name, also_print=True)


# Define a custom stream listener
class MastodonStreamListener(StreamListener):
    def __init__(self, instance_name):
        super().__init__()

        self.instance_name = instance_name
        self.ten_minutes_executed = False
        # Get the current date
        self.current_date = datetime.datetime.now().date()
        # Get the current hour
        self.current_hour = datetime.datetime.now().hour
        # Get the current file name
        self.file_name = self.get_exact_file_name()

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
            # Get the current month and date for the filename
            current_year_month = datetime.datetime.now().strftime("%Y-%m")
            current_date = datetime.datetime.now().strftime("%Y-%m-%d")

            instance_name = self.instance_name[len("https://"):]
            logger.info(f"start streaming data : {instance_name}")
            return os.path.join(backend_util.DATA_DERIVED_DIR, current_year_month, f"{current_date}", f"{instance_name}_{current_date}.json")

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
        #In this function is used to check if there is a new date started.
        now = datetime.datetime.now()
        if now.date() != self.current_date:
            self.end_of_day_gzip_create()


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
        }

        # Create directories for the current month and date if they don't exist
        os.makedirs(os.path.dirname(self.file_name), exist_ok=True)

        # write the data to the file.
        with open(self.file_name, 'a') as file:
            json.dump(toot_info, file, default=str)
            file.write('\n')

    def end_of_day_gzip_create(self):
        """
        This function is used to make a .gz file for each end of the day for each mastodon server.
        Parameters:
            None
        Returns:
            None
        .gz file is created, and the mastodon_{current_date}.json file is removed.
        """
        # Get the previous date
        previous_date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        previous_date_year_month = previous_date[:-3]

        previous_file_location = os.path.join(backend_util.DATA_DERIVED_DIR, previous_date_year_month, previous_date)

        # Construct the file path for the gzip file
        gzip_file_path = os.path.join(backend_util.DATA_DERIVED_DIR, previous_date_year_month,
                                      f"{previous_date}.tar.gz")

        try:
            # Create a tar archive from the directory
            with tarfile.open(gzip_file_path, 'w:gz') as tar:
                tar.add(backend_util.DATA_DERIVED_DIR, arcname=os.path.basename(backend_util.DATA_DERIVED_DIR))

            logger.info(f"Gzip file created successfully: {gzip_file_path}")

            try:
                # Delete the file after creation
                if os.path.exists(previous_file_location):
                    shutil.rmtree(previous_file_location)
                    logger.info(f"Directory deleted successfully: {previous_file_location}")
                    return
                else:
                    logger.info(f"Directory does not exist: {previous_file_location}")
            except Exception as e:
                logger.error(f"Error deleting file: {e}")
        except Exception as e:
            logger.error(f"Error creating gzip file: {e}")
        return


def stream_public_data(instance_info):
    # Create a Mastodon client
    mastodon_stream = Mastodon(
        access_token=instance_info['access_token'],
        api_base_url=instance_info['api_base_url']
    )
    # Use the access token for post streaming
    stream_listener = MastodonStreamListener(instance_info['api_base_url'])
    mastodon_stream.stream_public(stream_listener)