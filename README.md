# Mastodon Streamer
This project uses [Mastodon.py](https://mastodonpy.readthedocs.io/en/stable/index.html) to collect data from different Mastodon servers and save them to json files by date.


# quick start.
- get conda installed if not https://docs.anaconda.com/free/miniconda/#quick-command-line-install
- create conda venv
  - `conda env create -p ./<myvenv> -f environment.yml`
  - `conda activate ./<myvenv>`
  - `pip install -r requirements.txt`
- make a copy of `config.yml.template` and `mastodon_servers.json.template`, fill out the contents accordingly.
- run it `python streamer.py`
