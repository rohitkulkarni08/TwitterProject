#!/usr/bin/env python
# coding: utf-8

# In[1]:


import json
import tweepy


# In[3]:


def authorization(c_key,c_secret,a_token,a_secret):
    consumer_key=c_key
    consumer_secret=c_secret
    access_token=a_token
    access_token_secret=a_secret
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api=tweepy.API(auth)
    return;


# In[16]:


def tweet_retrieval(key):
    tweets_list = tweepy.Cursor(api.search, q=key,
                            since='2021-08-15', 
                            until='2021-08-7',
                            tweet_mode='extended',
                            lang="en").items(500)
    output2 = []
    for tweet in tweets_list:
        username=tweet.user.screen_name
        favourite_count = tweet.favorite_count
        retweet_count = tweet.retweet_count
        source = tweet.source
        line = {'twitter_id':tweet.id_str ,'username':username,'text' : tweet.full_text,'favourite_count':favourite_count, 'retweet_count' : retweet_count,
            'source': source,'location':tweet.user.location}
        output2.append(line)
    with open(key, "w") as outfile:
        json.dump(output2, outfile,indent=3)
    return outfile;

