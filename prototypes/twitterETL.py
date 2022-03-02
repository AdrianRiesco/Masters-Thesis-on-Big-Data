# Import libraries
import re
import json
import csv
import requests as rq
import pandas as pd

# Import functions from file
from functions import get_env, create_header, url_twitter_search_recent, send_get_request

# *** Twitter API - Extraction *** #

# Values from our query
keyword = "#NowPlaying" # Hashtag searched
max_results = 10 # Max results to gather (limit to keep API limit rates low)
since_id = None # Results must have a tweet id greater than this value

# Get keys and secrets
envalues = get_env()

# Create header
headers = create_header(envalues["TWITTER_API_BEARER_TOKEN"])

# Build the url
url = url_twitter_search_recent(keyword, max_results, since_id)

# Launch the query to the endpoint specified and get the result in json format
response = send_get_request(url[0], headers, url[1])

# *** Twitter API - Transformation *** #

# Create the dataframe to store the tweets
twitterDF = pd.DataFrame(columns = ['id', 'text', 'created_at'])

# Clean the output to obtain a text string without hashtags or urls
for tweet in response['data']:
    # Get id, text and created timestamp of the tweet
    tweet_id = tweet['id']
    tweet_text = tweet['text']
    tweet_created_at = tweet['created_at']
    
    # Clean the text of the tweet by removing its entities. We could use "annotations" entity to identify artist name, filtering by person class and probability, but it is not a completely reliable field
    # Create the list to store positions to remove
    removed_positions = []
    
    # Get cashtags positions
    if "cashtags" in tweet['entities']:
        for cashtag in tweet['entities']['cashtags']:
            start = cashtag['start']
            end = cashtag['end']
            removed_positions.append([start,end])
    
    # Get hasthags positions
    if "hashtags" in tweet['entities']:
        for hashtag in tweet['entities']['hashtags']:
            start = hashtag['start']
            end = hashtag['end']
            removed_positions.append([start,end])
    
    # Get mentions positions
    if "mentions" in tweet['entities']:
        for mention in tweet['entities']['mentions']:
            start = mention['start']
            end = mention['end']
            removed_positions.append([start,end])
    
    # Get urls positions
    if "urls" in tweet['entities']:
        for url in tweet['entities']['urls']:
            start = url['start']
            end = url['end']
            removed_positions.append([start,end])
    
    # Sort the removed_positions in descending order
    removed_positions.sort(key = lambda row:row[0], reverse = True)
    
    # Remove the characters from specific positions of the text
    for removed_position in removed_positions:
        tweet_text = tweet_text[0 : removed_position[0]] + tweet_text[removed_position[1] + 1 : :]
    
    # Remove all characters but Basic Latin by selecting unicode values less than 128
    tweet_text = ''.join([character for character in tweet_text if ord(character) < 128])
    
    # Remove other strange symbols excludying spaces
    tweet_text = re.sub('[\W_]+', ' ', tweet_text, flags = re.UNICODE)
    
    # Build the new row to be added (id, text, created_at)
    twitterDF_aux = pd.DataFrame({'id': [tweet_id], 'text': [tweet_text], 'created_at': [tweet_created_at]})

    # Append the row to the dataframe
    twitterDF = pd.concat([twitterDF, twitterDF_aux], ignore_index = True, axis = 0)

#lastID = twitterDF.iloc[-1]['id']
#print(lastID)

# DELETE next line after tests
twitterDF = pd.concat([twitterDF, pd.DataFrame(response['data'])], ignore_index = True, axis = 1)
    
# *** Twitter API - Load *** #

# Append the result to a CSV file or create if it does not exist
twitterDF.to_csv('tweets.csv', mode = 'a', index = False, header = None)

# *** Doubts *** #
# PENDING - Check if ID is already on csv and skip in that case -> No need to read CSV if keep last id stored -> Pending find way to keep lastID
# PENDING - Find way to compare Spotify result and see if the song is accurate
# PENDING - Better save to csv or launch query to spotify and only store once in a csv (or send to database)
# PENDING - Move transform and load to other file and import functions? Both are specific from Twitter so maybe there is no point on doing that
# PENDING - Option without csv -> Move "for tweet in response['data']" to functions.py and keep here shorter version with Twitter and Spotify processes
# PENDING - Some tokens expire after 30 days unused -> Twitter bearer doesn't, but maybe is better to request one if response fails
# Temporal files?
# Function names?


# Mantener '
