# Import libraries
from cassandra.cqlengine.models import Model
from models.base import Base

# Define Tweet model
class Tweet(Base):
    id_tweet = columns.UUID(primary_key=True, default=uuid.uuid4)
    text_tweet = columns.Text()
    created_at = columns.TimeUUID()
    url_tweet = columns.Text()
    id_track = columns.Text()
    name = columns.Text()
    popularity = columns.Integer()
    artists_id = columns.Text()
    artists_name = columns.Text()
    danceability = columns.Float()
    energy = columns.Float()
    key = columns.Integer()
    loudness = columns.Float()
    speechiness = columns.Float()
    acousticness = columns.Float()
    instrumentalness = columns.Float()
    liveness = columns.Float()
    valence = columns.Float()
    tempo = columns.Float()
    duration_ms = columns.Integer()
    time_signature = columns.Integer()
    mode = columns.Float()
    
    def get_data(self):
        return{
            'id_tweet': str(self.id_tweet),
            'text_tweet': self.text_tweet
        }
