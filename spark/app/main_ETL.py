# Import libraries
import os
import re
import sys
import csv
import json
import requests as rq
import pandas as pd
import sys
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, functions
from pyspark.sql.types import StringType

# Import functions from functions file
from functions import get_bearer_token_spotify, create_header, url_twitter_search_recent,  url_spotify_search_item, url_spotify_get_tracks_audio_features, url_spotify_get_several_artists, send_get_request

# Create Spark context
sc = SparkContext()

# Get the name and path of our csv
filename = sys.argv[1]

# *** Twitter API - Extraction *** #

# Values of our query
keyword = "#NowPlaying" # Hashtag searched
max_results = 50 # Max results to gather (limit to keep API limit rates low)
since_id = None # Results must have a tweet id greater than this value

# Open the csv with the data and get the last id stored, in case it exists
if os.path.exists(filename):
    with open(filename, mode='r', encoding='utf-8') as f:
        since_id = f.readlines()[-1].split(',')[0]

# Get keys and secrets
#envalues = get_env()

# Create header
headers = create_header(os.getenv("TWITTER_API_BEARER_TOKEN"))

# Build the url
url = url_twitter_search_recent(keyword, max_results, since_id)

# Launch the query to the endpoint specified and get the result in json format
response, status_code = send_get_request(url[0], headers, url[1])
if status_code != 200:
    sys.exit()
    
# *** Twitter API - Transformation *** #

# Create the dataframe to store the tweets
twitterDF = pd.DataFrame(columns = ['id_tweet', 'text', 'created_at'])

# Clean the output to obtain a text string without hashtags or urls
for tweet in response['data']:
    # Get id, text and created timestamp of the tweet
    tweet_id = tweet['id']
    tweet_text = tweet['text']
    tweet_created_at = tweet['created_at']
    
    # Clean the text of the tweet by removing its entities. "Annotations" entity could be used to identify artist name, filtering by person class and probability, but it is not completely reliable
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
    twitterDF_aux = pd.DataFrame({'id_tweet': [tweet_id], 'text': [tweet_text], 'created_at': [tweet_created_at]})

    # Append the row to the dataframe
    twitterDF = pd.concat([twitterDF, twitterDF_aux], ignore_index = True, axis = 0)

# DELETE next line after tests
# twitterDF = pd.concat([twitterDF, pd.DataFrame(response['data'])], ignore_index = True, axis = 1)

# *** Spotify API - Extraction & Transformation *** #

# Get Spotify bearer token (it expires every hour) and create header
bearer_token = get_bearer_token_spotify(os.getenv("SPOTIFY_API_CLIENT_ID"), os.getenv("SPOTIFY_API_CLIENT_SECRET"))['access_token']
headers = create_header(bearer_token)

# Values of our query
result_type = "track" # Receive only tracks as result
limit = 1 # Results to gather (by selecting 1 we only get the first result of the search)

# Create some variables to keep all the ids for tracks and artists
all_tracks_ids = ""
all_artists_ids = ""

# Create the dataframe to store previous information and the tracks
mainDF = pd.DataFrame(columns = ['id_tweet', 'text', 'created_at', 'url_tweet', 'id_track', 'name', 'popularity', 'artists_id', 'artists_name'])

# Create a for to launch consecutive queries, as "Search for Item" endpoint does not permit bulk searches
for i, tweet in enumerate(twitterDF.itertuples(index=False)):
    # Get the information of the dataframe
    id_tweet = tweet[twitterDF.columns.get_loc('id_tweet')]
    created_at = tweet[twitterDF.columns.get_loc('created_at')]
    keyword = tweet[twitterDF.columns.get_loc('text')]
    url_tweet = "https://twitter.com/author/status/" + id_tweet
    
    # Build the url
    url = url_spotify_search_item(keyword, result_type, limit)

    # Launch the query to the endpoint specified and get the result in json format
    response, status_code = send_get_request(url[0], headers, url[1])

    # Check the status code of the query
    if status_code == 200:
        # Check if response is not empty (no result found)
        if response['tracks']:
            if response['tracks']['items']:
                # Clean the output to obtain a text string without hashtags or urls
                for track in response['tracks']['items']:
                    # Get track id, name and popularity
                    track_id = track['id']
                    track_name = track['name']
                    track_popularity = track['popularity']

                    # Get artist id and name
                    artists_id = []
                    artists_name = []
                    for artist in track['artists']:
                        # Get artist id
                        artists_id.append(artist['id'])
                        artists_name.append(artist['name'].replace(","," "))
                    
                    # Separate ids and names with comma (in case there is more than one artist)   
                    artists_id = ','.join(artists_id)
                    artists_name = ','.join(artists_name)
                    
                    # Build the new row to be added (id, name, popularity, artists_id, artists_name)
                    mainDF_aux = pd.DataFrame({'id_tweet': [id_tweet], 'text': [keyword], 'created_at': [created_at], 'url_tweet': [url_tweet], 'id_track': [track_id], 'name': [track_name], 'popularity': [track_popularity], 'artists_id': ['{' + artists_id + '}'], 'artists_name': ['{' + artists_name + '}']})

                    # Append the row to the dataframe
                    mainDF = pd.concat([mainDF, mainDF_aux], ignore_index = True, axis = 0)
                    
                    # Add the tracks' ids to the string
                    all_tracks_ids = all_tracks_ids + track_id + ","

# After get song id, name and popularity and artist id and name, query again to Spotify to get tracks' features
keyword = all_tracks_ids[:-1]
# In case there is no tracks, the search was not successful and the script can be finished
if keyword == "":
    sys.exit()
url = url_spotify_get_tracks_audio_features(keyword)
response, status_code = send_get_request(url[0], headers, url[1])

# Create the dataframe to store the features
featuresDF = pd.DataFrame(columns = ['id', 'danceability', 'energy', 'key', 'loudness', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo', 'duration_ms', 'time_signature'])

# In case we get any response
if status_code == 200:
    if response['audio_features']:
        for features in response['audio_features']:
            # Get the different features of every track
            track_id = features['id']
            track_danceability = features['danceability']
            track_energy = features['energy']
            track_key = features['key']
            track_loudness = features['loudness']
            track_mode = features['mode']
            track_speechiness = features['speechiness']
            track_acousticness = features['acousticness']
            track_instrumentalness = features['instrumentalness']
            track_liveness = features['liveness']
            track_valence = features['valence']
            track_tempo = features['tempo']
            track_duration_ms = features['duration_ms']
            track_time_signature = features['time_signature']

            # Build the new row to be added (id, name, popularity, artists_id, artists_name)
            featuresDF_aux = pd.DataFrame({'id': [track_id], 'danceability': [track_danceability], 'energy': [track_energy], 'key': [track_key], 'loudness': [track_loudness], 'mode': [track_mode], 'speechiness': [track_speechiness], 'acousticness': [track_acousticness], 'instrumentalness': [track_instrumentalness], 'liveness': [track_liveness], 'valence': [track_valence], 'tempo': [track_tempo], 'duration_ms': [track_duration_ms], 'time_signature': [track_time_signature]})

            # Append the new row to the existing dataframe
            featuresDF = pd.concat([featuresDF, featuresDF_aux], ignore_index = True, axis = 0)

# Merge both dataframes by track id
mainDF = mainDF.join(featuresDF.set_index('id'), on='id_track')

# *** Load *** #

# Append the result to a CSV file or create it if does not exist
if not os.path.exists(filename):
    with open(filename, mode='w', encoding='utf-8') as f:
        f.write(','.join(mainDF.columns) + '\n')
mainDF.to_csv(filename, mode = 'a', encoding = 'utf-8', index = False, header = False)

# *** END *** #