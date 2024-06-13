"""
Purpose:
    This script is used to get the loggers into
Inputs:
    - No inputs to this file
Outputs:
    The mastodon configurations will be passed.
Author: Pasan Kamburugamuwa
"""

import os, sys,logging

# Log file location and the file
# LOG_DIR = "/Users/pkamburu/IUNI/mastodon_log"
# LOG_FNAME = "mastodon_streamer_logging.log"
# DATA_DERIVED_DIR = "/Users/pkamburu/IUNI/mastodon_derived_data"

LOG_DIR = "mastodon_log"
LOG_FNAME = "mastodon_streamer_logging.log"
DATA_DERIVED_DIR = "mastodon_derived_data"

def get_logger(log_dir, log_fname, script_name=None, also_print=False):
    """
    Create logger for the project.
    """
    # Create log_dir if it doesn't exist already
    try:
        os.makedirs(f"{log_dir}")
    except:
        pass

    # Create logger and set level
    logger = logging.getLogger(script_name)
    logger.setLevel(level=logging.INFO)

    # Configure file handler
    formatter = logging.Formatter(
        fmt="%(asctime)s-%(name)s-%(levelname)s - %(message)s",
        datefmt="%Y-%m-%d_%H:%M:%S",
    )
    full_log_path = os.path.join(log_dir, log_fname)
    fh = logging.FileHandler(f"{full_log_path}")
    fh.setFormatter(formatter)
    fh.setLevel(level=logging.INFO)
    # Add handlers to logger
    logger.addHandler(fh)

    # If also_print is true, the logger will also print the output to the
    # console in addition to sending it to the log file
    if also_print:
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(formatter)
        ch.setLevel(level=logging.INFO)
        logger.addHandler(ch)
    return logger