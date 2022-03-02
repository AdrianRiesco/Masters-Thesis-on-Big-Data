# Import libraries
import re
import json
import csv
import requests as rq
import pandas as pd

# Import functions from file
from functions import get_env, get_bearer_token_spotify, create_header, url_spotify_search_item, url_spotify_get_tracks_audio_features, url_spotify_get_several_artists, send_get_request

# *** Spotify API - Extraction & Transformation *** #

# Get keys and secrets
envalues = get_env()

# Get bearer token (it expires every hour) and create header
bearer_token = get_bearer_token_spotify(envalues["SPOTIFY_API_CLIENT_ID"], envalues["SPOTIFY_API_CLIENT_SECRET"])['access_token']
headers = create_header(bearer_token)

# Values from our query
#keywords = ["Green Valley si no te tengo", "Morodo divina ciencia"]
result_type = "track" # We want to receive songs as result
limit = 1 # Results to gather (by selecting 1 we only get the first result of the search)

# Create the dataframe to store the tracks
tracksDF = pd.DataFrame(columns = ['id', 'name', 'popularity', 'artists_id', 'artists_name'])

# Create some variables to keep all the ids for tracks and artists
all_tracks_ids = ""
all_artists_ids = ""

# Get tweets' data from the csv file
#tweetsDF = pd.read_csv('tweets.csv', header = 0, sep = ',', index_col='id', names = ["id", "text", "created_at"])
tweetsDF = pd.read_csv('tweets.csv', header = 0, sep = ',', index_col='id', names = ["id", "text", "created_at", "a", "b", "c", "d"])

print(tweetsDF['text'])
# Create a for to launch consecutive queries, as "Search for Item" endpoint does not permit bulk searches
#for keyword in keywords:
for keyword in tweetsDF['text']:
    # Build the url
    url = url_spotify_search_item(keyword, result_type, limit)

    # Launch the query to the endpoint specified and get the result in json format
    response = send_get_request(url[0], headers, url[1])

    # Check if response is not empty
    if response['tracks']['items']:
        print("+Found result for " + keyword)
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
            
            ########## Cambiar formato de , a otro distinto -> Meter dentro de { o [
            ########## Buscar si país artista o canción
            
            # Build the new row to be added (id, name, popularity, artists_id, artists_name)
            tracksDF_aux = pd.DataFrame({'id': [track_id], 'name': [track_name], 'popularity': [track_popularity], 'artists_id': [artists_id], 'artists_name': [artists_name]})

            # Append the row to the dataframe
            tracksDF = pd.concat([tracksDF, tracksDF_aux], ignore_index = True, axis = 0)
            
            # Add the track and artists ids to the string
            all_tracks_ids = all_tracks_ids + track_id + ","
            all_artists_ids = all_artists_ids + artists_id + ","
    else:
        print("-No result for " + keyword)

# After get song id, name and popularity and artist id and name, query again to Spotify to get tracks' features
keyword = all_tracks_ids[:-1]
url = url_spotify_get_tracks_audio_features(keyword)
response = send_get_request(url[0], headers, url[1])

# Create the dataframe to store the features
featuresDF = pd.DataFrame(columns = ['id', 'danceability', 'energy', 'key', 'loudness', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo', 'type', 'duration_ms', 'time_signature'])

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
    track_type = features['type']
    track_duration_ms = features['duration_ms']
    track_time_signature = features['time_signature']

    # Build the new row to be added (id, name, popularity, artists_id, artists_name)
    featuresDF_aux = pd.DataFrame({'id': [track_id], 'danceability': [track_danceability], 'energy': [track_energy], 'key': [track_key], 'loudness': [track_loudness], 'mode': [track_mode], 'speechiness': [track_speechiness], 'acousticness': [track_acousticness], 'instrumentalness': [track_instrumentalness], 'liveness': [track_liveness], 'valence': [track_valence], 'tempo': [track_tempo], 'type': [track_type], 'duration_ms': [track_duration_ms], 'time_signature': [track_time_signature]})

    # Append the row to the dataframe
    featuresDF = pd.concat([featuresDF, featuresDF_aux], ignore_index = True, axis = 0)

# Merge the dataframes from both queries
spotifyDF = tracksDF.join(featuresDF.set_index('id'), on='id')

# *** Twitter API - Load *** #

# Append the result to a CSV file or create if it does not exist
spotifyDF.to_csv('spotify.csv', mode='a', index = False, header = None)

# *** Doubts *** #
# PENDING - Problem if we only get artist name -> Get first song received (check from tweets' text)
# PENDING - Refresh bearer token before queries (it expires in 1 hour) -> Update every time or check if expired and update in that case? -> Safe current bearer in a variable
# PENDING - Collaborations between several artists? Should we save artist of same song separated by commas?
# PROBLEM - Check how to split artist name in DataFrame and when persisted -> Commas can be confusing if there is an artist name with a comma. Maybe /n? Or clean the name and remove commas and then add to separate?
# PROBLEM - Problem with more than one artist, each of one has several genres.
# PENDING - Add cases for possible errors -> Empty responses, wrong response (different code than 200)...
# PROBLEM - Don't find all songs. Maybe process everything and select only queries with results?
# Check if song or artist id already stored to avoid repeating queries? For how long should we keep that "cache"?
# No problem if one id is the same -> Result will be received several times but the query is still the same -> No need to filter that

'''
# Launch the new query to the "Get Several Artists" endpoint
all_artists_ids = "1v7iZcyrm4fHfsEBiseomy,2OnH4HpywAxWkSOEsyjdjn,"
keyword = all_artists_ids[:-1]
url = url_spotify_get_several_artists(keyword)
response = send_get_request(url[0], headers, url[1])

# Create the dataframe to store the features
artistsDF = pd.DataFrame(columns = ['id', 'genres', 'popularity', 'name'])

for artist in response['artists']:
    # Get the different features of every track
    artist_id = artist['id']
    artist_genres = ""
    for genre in artist['genres']:
        artist_genres = artist_genres + genre + ","
        
    artist_popularity = artist['popularity']
    artist_name = artist['name']
    track_loudness = features['loudness']
    track_mode = features['mode']
    track_speechiness = features['speechiness']
    track_acousticness = features['acousticness']
    track_instrumentalness = features['instrumentalness']
    track_liveness = features['liveness']
    track_valence = features['valence']
    track_tempo = features['tempo']
    track_type = features['type']
    track_duration_ms = features['duration_ms']
    track_time_signature = features['time_signature']

    # Build the new row to be added (id, name, popularity, artists_id, artists_name)
    artistsDF_aux = pd.DataFrame({'id': [artist_id], 'genres': [artist_genres], 'popularity': [artist_popularity], 'name': [artist_name]})

    # Append the row to the dataframe
    artistsDF = pd.concat([artistsDF, artistsDF_aux], ignore_index = True, axis = 0)
    
print("Artists")
print(artistsDF)
'''
