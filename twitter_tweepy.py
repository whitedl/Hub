#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Twitter API Streaming
"""

from __future__ import annotations
import datetime
import requests
from meerschaum.utils.typing import SuccessTuple, Dict, List, Any, Optional
import translators as ts

__version__ = '0.0.2'

required = ['requests', 'pandas', 'numpy', 'tweepy', 'pytz']

# Registers the plugin with meerschaum when it's added as a pipe. Only run once.
def register(pipe, **kw):
    from meerschaum.config import get_plugin_config, write_plugin_config
    from meerschaum.utils.prompt import prompt

    # Ask for the API token and store for future use.
    cf = get_plugin_config(warn=False)
    if cf is None:
        cf = {}
    if cf.get('token', None) is None:
        token = prompt("Please enter your Twitter API token:")
        cf['token'] = token
        write_plugin_config(cf)

    # Return pipe parameters.
    return {
        'columns': {
            'datetime': 'timestamp',
        },
    }

# Runs each time the plugin retrieves data.
def fetch(
        pipe,
        begin: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        **kw
    ):
    from meerschaum.config import get_plugin_config
    import pandas as pd
    import json
    import numpy as np
    import tweepy
    import pytz

    # Get bearer token and init tweepy client
    token = get_plugin_config()['token']
    client = tweepy.Client(bearer_token=token)

    # datatypes constant for later use
    dtypes = {
        'timestamp': 'datetime64[ns]',
        'text': str,
        'username': str,
        'tags': object,
        'urls': object,
        'lang': str,
        'user_reported_location': str,
        'id': 'int64',
        'retweet_count': 'int64',
        'places_mentioned': object,
        'user_created_at': 'datetime64[ns]',
        'tweet_count': 'int64',
        'user_desc': str,
        'verified': bool,
        'user_loc_lat': 'float64',
        'user_loc_long': 'float64',
        'user_age': 'int32'
    }

    # Set twitter search query
    query = 'xinjiang'

    # Information to get about the tweets and users
    tweet_fields = ['context_annotations', 'created_at', 'entities', 'public_metrics', 'source', 'geo', 'lang']
    expansions = ['author_id', 'geo.place_id']
    place_fields = ['full_name', 'id', 'geo', 'place_type']
    user_fields = ['id', 'name', 'username', 'location', 'public_metrics', 'verified', 'description', 'created_at']

    # Set maximum number of results to retrieve
    max_results = 10

    tweets_df = pd.DataFrame()

    # Get recent tweets
    results = client.search_recent_tweets(query=query, tweet_fields=tweet_fields,
        expansions=expansions, max_results=max_results, place_fields=place_fields, user_fields=user_fields)

    # Set up user dict for ease of access
    if 'users' in results.includes:
        users = {u["id"]: u for u in results.includes['users']}

    # If we got tweets:
    if results.data:
        # Loop through each tweet
        for tweet in results.data:
            # Set up user dict with so we can easily get user values
            user = users[tweet.author_id]
            # Get data from tweet
            tweet_data = {
                'timestamp': tweet.created_at,
                'text': tweet.text,
                'username': user.username,
                'urls': None,
                'tags': None,
                'lang': tweet.lang,
                'id': tweet.id,
                'retweet_count': tweet.public_metrics['retweet_count'],
                'user_reported_location': user.location,
                'user_created_at': user.created_at,
                'tweet_count': user.public_metrics['tweet_count'],
                'places_mentioned': None,
                'user_desc': user.description,
                'verified': user.verified,
                'user_loc_lat': None,
                'user_loc_long': None,
                'user_age': (datetime.datetime.now(pytz.utc) - user.created_at).days
            }

            # Some tweets have extra information like hashtags and user locations.
            # If the tweet has extra info
            if tweet.entities:
                # If there were urls
                if 'urls' in tweet.entities:
                    # Get the urls from the tweet entities
                    tweet_data['urls'] = [url['expanded_url'] for url in tweet.entities['urls']]
                # If there were hashtags
                if 'hashtags' in tweet.entities:
                    # Get the hashtags from the tweet entities
                    tweet_data['tags'] = [tag_info['tag'] for tag_info in tweet.entities['hashtags']]
                # If there were annotations on the tweet
                if 'annotations' in tweet.entities:
                    # Search for places mentioned in the annotations
                    places = [
                        place_info['normalized_text']
                        for place_info in tweet.entities['annotations']
                        if place_info['type'] == 'Place'
                    ]
                    # If there were places mentioned, set that in the dict
                    tweet_data['places_mentioned'] = places if len(places) else None

            # Get geocoded lat and long from the user's reported location if they supplied one
            if tweet_data['user_reported_location']:
                # Get geocode response
                pos = geocode(tweet_data['user_reported_location'])
                # Process geocode response
                if pos:
                    pos = pos[0]['position']
                    tweet_data['user_loc_lat'] = pos['lat']
                    tweet_data['user_loc_long'] = pos['lng']

            # Make df from tweet for concatenation
            tweet_df = pd.DataFrame([tweet_data])
            # Concat tweet with existing tweets
            tweets_df = pd.concat([tweets_df, tweet_df], ignore_index=True)

    # Reorder columns
    tweets_reordered = tweets_df[['timestamp','text','username','tags','urls','lang','user_reported_location','id','retweet_count','places_mentioned','user_desc','user_created_at','tweet_count','user_loc_lat','user_loc_long','user_age','verified']]

    # Set column data types
    tweets_final = tweets_reordered.astype(dtypes)

    return tweets_final

# Sync calls fetch and does extra processing on the fetched data.
def sync(pipe, **kw):
    import meerschaum as mrsm

    parent_data = fetch(pipe, **kw)

    pipe.sync(parent_data, **kw)
    # Create child pipe, adding a column for translated text
    child_pipe = mrsm.Pipe(
        pipe.connector_keys,
        pipe.metric_key,
        'expanded',
        columns = pipe.columns
    )
    # copy fetched data to child data and add translated text column
    child_data = parent_data.assign(
        # translate each non-english tweet
        translated=lambda row: translate(row.text, row.lang) if row.lang != 'en' else ""
    )
    # Add the fetched and additional data to the child pipe
    return child_pipe.sync(child_data, **kw)

# Translate text in a given language
def translate(text, lang):
    return ts.google(text, from_language=lang)

# Get the geocode information from a place name string
def geocode(location):
    # Set URL
    geocode_url = f"https://geocode.search.hereapi.com/v1/geocode?q={location}&apikey=nacXHor6-tXlB5zT96YYP0iWi83i6E-kntlARVQOK48"
    try:
        # Request geocoded response
        results = requests.get(geocode_url)
    except requests.exceptions.RequestException as err:
        # If the geocoder errors, return none.
        print(f"Could not find location for: {location}")
        return None
    else:
        results = results.json()
        # If the geocoder couldn't geocode the location, return none.
        if 'items' not in results:
            print(f"Could not find location for: {location}")
            return None
        # Return the geocoded location.
        return results['items']