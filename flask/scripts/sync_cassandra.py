# Import libraries
from cassandra.cqlengine import connection
from cassandra.cqlengine.management import sync_table
from models.tweet import Tweet

# Define connection to Cassandra
connection.setup(['cassandra-1'], "mainkeyspace", 1)

# Sync with Tweet model
sync_table(Tweet)
