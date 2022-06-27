# Libraries
from flask import Flask, redirect, url_for, render_template, request, flash, session
from flask_cqlalchemy import CQLAlchemy
import datetime
import uuid
import json
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

# Object to be synchronized with Cassandra
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

	@property
	def list(self):
		return [self.id_tweet, self.created_at.strftime("%m/%d/%Y %H:%M:%S"), self.url_tweet, self.name, self.artists_name.replace("{", "").replace("}", ""),
		self.popularity, round(self.danceability*100, 3), round(self.energy*100, 3), self.key, round(self.loudness, 3), round(self.speechiness*100, 3), round(self.acousticness*100, 3),
		round(self.instrumentalness*100, 3), round(self.liveness*100, 3), round(self.valence*100, 3), round(self.tempo, 3), round(self.duration_ms/1000, 0), self.time_signature, round(self.mode, 0)]

	@property
	def column_list(self):
		return ["id_tweet", "created_at", "url_tweet", "name", "artists_name", "popularity", "danceability", "energy", "key", "loudness", "speechiness", "acousticness",
		"instrumentalness", "liveness", "valence", "tempo", "duration_ms", "time_signature", "mode"]

# Home page
@app.route("/")
def home():
	return render_template("index.html")

# Table view
@app.route("/data", methods = ['GET'])
def data():
	data_list = [track.list for track in Tweetsandtracks.objects()]

	return render_template("data.html", data_list = data_list)

# Chart view
@app.route("/visuals", methods = ['GET', 'POST'])
def visuals():	
	data_columns = Tweetsandtracks().column_list
	data_list = [track.list for track in Tweetsandtracks.objects()]

	return render_template("visuals.html", data_list = data_list, data_columns = data_columns)

if __name__ == "__main__":
	db.sync_db()
	db.set_keyspace(app.config['CASSANDRA_KEYSPACE'])
	app.run(debug = True)