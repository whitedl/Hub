#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Twitter API Streaming
"""

from __future__ import annotations
import datetime
from meerschaum.utils.typing import SuccessTuple, Dict, List, Any, Optional

__version__ = '0.0.1'

required = ['requests', 'pandas', 'numpy']


def register(pipe, **kw):
    from meerschaum.config import get_plugin_config, write_plugin_config
    from meerschaum.utils.prompt import prompt

    ### Ask for the API token and store for future use.
    cf = get_plugin_config(warn=False)
    if cf is None:
        cf = {}
    if cf.get('token', None) is None:
        token = prompt("Please enter your Twitter API token:")
        cf['token'] = token
        write_plugin_config(cf)

    ### Return pipe parameters.
    return {
        'columns': {
            'datetime': 'created_at',
        },
    }


def fetch(
        pipe,
        begin: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        **kw
    ):
    from meerschaum.config import get_plugin_config
    import pandas as pd
    import requests
    import json
    import numpy as np
    token = get_plugin_config()['token']
    headers = {
        'Authorization': f'Bearer {token}',
    }
    url = "https://api.twitter.com/2/tweets/search/recent"
    query_params = {
        'query': 'xinjiang',
        'max_results': 10,
            'expansions': 'author_id,geo.place_id',
            'tweet.fields': 'created_at,lang,entities,public_metrics',
            'user.fields': 'public_metrics,location',
            'place.fields': 'country'
    }

    dtypes = {
        'id': str,
        'created_at': 'datetime64[ns]',
        'author_id': str,
        'username': str,
        'name': str,
        'lang': str,
        'text': str,
        'entities.annotations': object,
        'entities.mentions': object,
        'retweet_count': str,
        'public_metrics.quote_count': str,
        'entities.hashtags': object,
        'entities.urls': object,
        'tags': str,
        'urls': str,
        'user_reported_location': str,
        'place': str,
    }


    ### I haven't changed much of the logic below, just some touchups.
    ### The important changes is the enforcement of types (`astype` at the end).


    response = requests.get(url, params=query_params, headers=headers).json()

    tweet_user = response['includes']
    tweet_user = pd.json_normalize(tweet_user,record_path=['users'])
    tweet_user.rename(columns = {'id':'author_id','location':'user_reported_location'}, inplace = True)
    tweet_user.reindex(columns=['author_id','name','username','user_reported_location'])


    tweet_data = response['data']
    tweet_data = pd.json_normalize(tweet_data)
    tweet_data.rename(columns = {'public_metrics.retweet_count':'retweet_count'}, inplace = True)
    tweet_data = tweet_data.reindex(columns=['id','created_at','author_id','lang','text','entities.annotations','entities.mentions','retweet_count','public_metrics.reply_count',
    'public_metrics.reply_count','public_metrics.quote_count','entities.hashtags','entities.urls','country'])


    # Checks for hashtags and creates a tags column and creates simple lists of tags
    if (tweet_data.columns == 'entities.hashtags').any():

        tags = []
        for value in tweet_data['entities.hashtags']:
            
            if type(value) == float and pd.isna(value):
                tags.append(np.nan)
            else:
                lst = [a["tag"] for a in value]
                stripped = str(lst).replace('[','').replace(']','').replace('\'','').replace('\"','')
                #tags.append(tag_list_strip(lst)) this is function that is not quite working see above
                tags.append(stripped)

        tweet_data['tags'] = tags
    else:
        tweet_data['tags']= np.nan

    # Checks for URLs and creates a URLs column and creates simple lists of URLs
    if (tweet_data.columns == 'entities.urls').any():
        urls = []
        for value in tweet_data['entities.urls']:
            if type(value) == float and pd.isna(value):
                urls.append(np.nan)
            else:
                lst = [a["url"] for a in value]
                stripped = str(lst).replace('[','').replace(']','').replace('\'','').replace('\"','')
                #urls.append(url_list_strip(lst)) this is function that is not quite working see above
                urls.append(stripped)

        tweet_data['urls'] = urls         
    else:
        tweet_data['urls']= np.nan

    # Parse entities.annotations for Twitter Place Value if present
    place = []
    for value in tweet_data['entities.annotations']:
        if type(value) == float and pd.isna(value):
            place.append(np.nan)

        else: 
            if value[0]['type'] == 'Place':
                place.append(value[0]['normalized_text'])
                
            else: 
                place.append(np.nan)

    tweet_data['place'] = place

    # Creates the finall data frame and merges tweet data and user data, specifying which columns to keep and order as API V2 is not consistent about attributre
    #  position in the JSON response.
    result = pd.merge(tweet_data, tweet_user.rename(columns={'id':'author_id'}), on='author_id',  how='left')
    result_reorder = result[['id','created_at','author_id','username','name','lang','text','entities.annotations','entities.mentions','retweet_count','public_metrics.quote_count','entities.hashtags','entities.urls','tags','urls','user_reported_location','place']]

    #REMOVE THIS
    print('SUCCESS')
    # Set data types
    result_reorder = result_reorder.astype(dtypes)
    #result_reorder.info()
    return result_reorder
