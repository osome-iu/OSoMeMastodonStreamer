# Mastodon Streamer
This project is used to collect Mastodon data from different servers and save them as gzip file at the end of the day.


# Project Structure
- Backend: A [flask](https://flask.palletsprojects.com/en/2.3.x/) application that leverages the [[mastodonpy](https://mastodonpy.readthedocs.io/en/stable/index.html)) to retrieve data and manipulate the data.


## Packages
- [Mastodon.py](https://mastodonpy.readthedocs.io/en/stable/index.html)


# How to run the project.
To run the project, you have to clone the project.
Edit the [mastodon_servers.json](https://github.com/osome-iu/OSoMeMastodonStreamer/blob/main/library/mastodon_servers.json)  and add as below json.

```
  {
   "mastodon_servers":[
     {
       "access_token":"XXXX",
       "api_base_url":"https://mastodon.social"
     },
     {
       "access_token":"XXX",
       "api_base_url":"https://mastodon.cloud"
     },
     {
       "access_token":"XXX",
       "api_base_url":"https://genomic.social"
     }  
   ]
  }
```

Then,
  1. Install necessary pip packages. Run `pip install -r requirements.txt`.
  2. Run your flask app by running `flask run --port <desired_port_number>`.

