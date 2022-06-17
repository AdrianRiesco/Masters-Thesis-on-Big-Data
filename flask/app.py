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
	id_tweet = db.columns.UUID(primary_key=True, default=uuid.uuid4)
	text_tweet = db.columns.Text()
	created_at = db.columns.TimeUUID()
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

# Get new Cassandra session
def get_new_cassandra_session():
	""" This function returns the connection to the MySQL database

	Returns:
		MySQLConnection

	"""

	return ""

# Execute a Cassandra query
def executeQuery(cql, par, option):
	""" This function execute a query with the cql and parameters sent and option selected

	Args:
		cql (string): CQL statement for the query
		par (tuple): arguments to include in the SQL query
		option (string): type of query to send

	Returns:
		response (list of tuples) : response from the query (empty if commiting changes to the database)

	"""
	'''
	session = get_new_cassandra_session() 

	# Create connection and cursor
	db = getMysqlCon()
	my_cursor = db.cursor()

	# Execute query
	my_cursor.execute(cql, par)

	# Get the response or commit the changes
	if option == "get_all":
		response = my_cursor.fetchall()
	elif option == "get_one":
		response = my_cursor.fetchone()
	elif option == "modify":
		response = []
		db.commit()

	# Close cursor and connection
	my_cursor.close()
	db.close()
	'''

	return ""

# Home page
@app.route("/")
def home():
	print('Database synchronized', flush = True)
	for p in Tweetsandtracks.objects().limit(100):
		print('bucle', flush = True)
    		print(p, flush = True)

	return render_template("index.html", tracks = Tweetsandtracks.objects())

if __name__ == "__main__":
	db.sync_db()
	db.set_keyspace(app.config['CASSANDRA_KEYSPACE'])
	app.run()