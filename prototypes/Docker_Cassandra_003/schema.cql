DROP KEYSPACE IF EXISTS mainkeyspace;

CREATE KEYSPACE mainkeyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE mainkeyspace;

CREATE TABLE TweetsAndTracks(id_tweet int PRIMARY KEY, created_at timestamp, url_tweet text, id_track int, name text, popularity int, artists_id text, artists_name text, danceability float, energy float, key int, loudness float, speechiness int, acousticness float, instrumentalness float, liveness float, valence float, tempo float, duration_ms int, time_signature int);