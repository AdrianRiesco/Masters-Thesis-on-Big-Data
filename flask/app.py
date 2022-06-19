# Import libraries
from flask import Flask, redirect, url_for, render_template, request, flash, session
from flask_cqlalchemy import CQLAlchemy
import uuid
import sys
import os

app = Flask(__name__)

# App configuration
app.config['CASSANDRA_USER'] = os.getenv("CASSANDRA_USER","cassandra")
app.config['CASSANDRA_PASSWORD'] = os.getenv("CASSANDRA_PASSWORD","cassandra")
app.config['CASSANDRA_HOSTS'] = {os.getenv("CASSANDRA_HOSTS","cassandra-1")}
app.config['CASSANDRA_KEYSPACE'] = os.getenv("CASSANDRA_KEYSPACE","mainkeyspace")
app.config['CASSANDRA_SETUP_KWARGS'] = {'port': 9042}

db = CQLAlchemy(app)

class Tweetsandtracks(db.Model):
	__keyspace__ = app.config['CASSANDRA_KEYSPACE']
	id_tweet = db.columns.Integer(primary_key=True)
	text_tweet = db.columns.Text()
	created_at = db.columns.DateTime()
	url_tweet = db.columns.Text()
	id_track = db.columns.Text()
	name = db.columns.Text()
	popularity = db.columns.Integer()
	artists_id = db.columns.Text()
	artists_name = db.columns.Text()
	danceability = db.columns.Float()
	energy = db.columns.Float()
	key = db.columns.Integer()
	loudness = db.columns.Float()
	speechiness = db.columns.Float()
	acousticness = db.columns.Float()
	instrumentalness = db.columns.Float()
	liveness = db.columns.Float()
	valence = db.columns.Float()
	tempo = db.columns.Float()
	duration_ms = db.columns.Integer()
	time_signature = db.columns.Integer()
	mode = db.columns.Float()

# Home page
@app.route("/")
def home():
	print('Database synchronized', flush = True)
	#for p in Tweetsandtracks.objects().limit(100):
    	#	print(p, flush = True)

	return render_template("index.html")

# Table page
@app.route("/table")
def table():
	return render_template("table.html", tracks = Tweetsandtracks.objects())

# Graph page
@app.route("/chart", methods = ['GET', 'POST'])
def chart():
	# Get the selected view
	view = request.args.get('view')

	# Select the data for the selected view
	if view == "TopListenedTracks":
		# Data for the top listened tracks
		title = "Top 10 Listened Tracks"
		label_name = ["Tweets found (0-100)"]

		data_labels = [tat.name for tat in Tweetsandtracks.objects()]
		data_values = [data_labels.count(label) for label in data_labels]

		data_labels = [label for _, label in sorted(zip(data_values, data_labels), reverse=True)][:10]
		data_values = sorted(data_values, reverse=True)[:10]
	elif view == "TopDanceableTracks":
		# Data for the top danceable tracks
		title = "Top 10 Danceable Tracks"
		label_name = ["Danceability (0-1)"]

		data_labels = [tat.name for tat in Tweetsandtracks.objects()]
		data_values = [float(round(tat.danceability, 2)) for tat in Tweetsandtracks.objects()]

		data_labels = [label for _, label in sorted(zip(data_values, data_labels), reverse=True)][:10]
		data_values = sorted(data_values, reverse=True)[:10]
	else:
		# Data for the top popular tracks and the default view, in case the "view" variable is None (accesed via navbar)
		title = "Top 10 Popular Tracks"
		label_name = ["Popularity (0-100)"]

		data_labels = [tat.name for tat in Tweetsandtracks.objects()]
		data_values = [float(round(tat.popularity, 2)) for tat in Tweetsandtracks.objects()]

		data_labels = [label for _, label in sorted(zip(data_values, data_labels), reverse=True)][:10]
		data_values = sorted(data_values, reverse=True)[:10]
	
	print(data_labels, flush = True)
	print(data_values, flush = True)

	#for i in range(len(data_labels)):
    	#	print(str(i) + ". " + str(data_labels[i]) + " - " + str(data_values[i]), flush = True)

	# Top canciones populares - Y
	# Top canciones más escuchadas
	# Top canciones con danceability - Y
	# Top canciones
	# Count of songs per range of metric

	return render_template("chart.html", title = title, label_name = label_name, data_labels = data_labels, data_values = data_values)

if __name__ == "__main__":
	db.sync_db()
	db.set_keyspace(app.config['CASSANDRA_KEYSPACE'])
	app.run()