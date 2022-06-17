# Import libraries
from cassandra.cqlengine.models import Model

# Define Base model
class Base(Model):
    __abstract__ = True
    __keyspace__ = "mainkeyspace"
