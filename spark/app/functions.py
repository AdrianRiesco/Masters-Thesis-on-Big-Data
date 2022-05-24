# Import libraries needed
import json
import requests as rq
import pandas as pd

def get_bearer_token_spotify(client_id, client_secret):
    endpoint_url = "https://accounts.spotify.com/api/token"
    data = {"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret}
    response = rq.request("POST", endpoint_url, data = data)
    #if response.status_code != 200:
        #print("Endpoint Response Code: " + str(response.status_code))
        #raise Exception(response.status_code, response.text)
    return response.json()

# Create the authentication header for the query	
def create_header(bearer_token):
    header = {"Authorization": "Bearer " + bearer_token}
    return header

# Create the url for the query to the Twitter endpoint Search Recent
def url_twitter_search_recent(q, max_r = 10, sid = None):
    endpoint_url = "https://api.twitter.com/2/tweets/search/recent"
    query_parameters = {'query': q, 'max_results': max_r, 'since_id': sid, 'tweet.fields': 'id,text,created_at,entities'}            
    return (endpoint_url, query_parameters)

# Create the url for the query to the Spotify endpoint Search for Item
def url_spotify_search_item(q, result_type = "track", limit = 1):
    endpoint_url = "https://api.spotify.com/v1/search"
    query_parameters = {'query': q, 'type': result_type, 'limit': limit}            
    return (endpoint_url, query_parameters)

# Create the url for the query to the Spotify endpoint Get Tracks' Audio Features
def url_spotify_get_tracks_audio_features(ids):
    endpoint_url = "https://api.spotify.com/v1/audio-features"
    query_parameters = {'ids': ids}            
    return (endpoint_url, query_parameters)
    
# Create the url for the query to the Spotify endpoint Get Several Artists
def url_spotify_get_several_artists(ids):
    endpoint_url = "https://api.spotify.com/v1/artists"
    query_parameters = {'ids': ids}            
    return (endpoint_url, query_parameters)

# Initial connection to the endpoint
def send_get_request(url, headers, params):
    response = rq.request("GET", url, headers = headers, params = params)
    #if response.status_code != 200:
        #print("Endpoint Error Response Code: " + str(response.status_code))
        #raise Exception(response.status_code, response.text)
    return response.json(), response.status_code
