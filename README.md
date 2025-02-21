# Mastodon Streamer
This project uses [Mastodon.py](https://mastodonpy.readthedocs.io/en/stable/index.html) to collect data from different Mastodon servers and save them to json files by date.


# quick start.
- get conda installed if not https://docs.anaconda.com/free/miniconda/#quick-command-line-install
- create conda venv
  - `conda env create -p ./<myvenv> -f environment.yml`
  - `conda activate ./<myvenv>`
  - `pip install -r requirements.txt`
- make a copy of `config.yml.template` and `mastodon_servers.json.template`, fill out the contents accordingly.
- run
  - `python streamer.py` saves to `{base_folder}/{yyyy-mm}/{domain}_{yyyy-mm-dd}.json`
  - `python stream_new_users.py` saves to `{base_folder}/{yyyy-mm}/new_users/{domain}_{yyyy-mm-dd}_new_users.json`

---

*optional backup bash script uses python package `yq` that wraps around `jq` 

`sudo apt-get install jq` `pip3 install yq`
