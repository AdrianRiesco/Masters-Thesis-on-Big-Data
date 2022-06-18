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
@app.route("/chart")
def chart():
	data_labels = [tat.id_tweet for tat in Tweetsandtracks.objects()]
	data_values = [tat.energy for tat in Tweetsandtracks.objects()]
	return render_template("chart.html", data_labels = data_labels, data_values = data_values)

if __name__ == "__main__":
	db.sync_db()
	db.set_keyspace(app.config['CASSANDRA_KEYSPACE'])
	app.run()