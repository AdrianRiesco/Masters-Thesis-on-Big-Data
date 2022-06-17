# Import libraries
from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

# *** Spark parameters
spark_master = "spark://spark:7077"
spark_app_name = "Spark_Data_Extraction"
file_path = "/opt/spark/resources/data.csv"
cassandra_host = "cassandra-1"
cassandra_password = "cassandra"
cassandra_username = "cassandra"
cassandra_keyspace = "mainkeyspace"

# *** DAG
now = datetime.now()
history_filename = now.strftime('%Y%m%d%H%M%S') + ".csv"

# Description of the Direct Acyclic Graph
dag = DAG(
    dag_id = "spark_main", 
    description = "This DAG runs the principal Pyspark app to extract data from Twitter and Spotify and store it in Cassandra",
    default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes = 1)},
    start_date = datetime(2022, 1, 1), # Could be changed for: datetime(now.year, now.month, now.day)
    schedule_interval = timedelta(minutes = 30),
    catchup = False
)

# Dummy operator to add an initial step
start = DummyOperator(task_id = "start", dag = dag)

# Spark job to extract the data from Twitter and Spotify, and transform and store it in a .csv file
spark_submit = SparkSubmitOperator(
    task_id = "spark_job",
    application = "/opt/spark/app/main_ETL.py",
    name = spark_app_name,
    conn_id = "spark_conn",
    verbose = 1,
    conf = {"spark.master":spark_master},
    application_args = [file_path],
    dag = dag
)

# Bash command to load the .csv file to Cassandra
cassandra_load = BashOperator(
    task_id = "cassandra_load",
    bash_command = "cqlsh %s -u %s -p %s -k %s -e \"COPY TweetsAndTracks(id_tweet, text_tweet, created_at, url_tweet, id_track, name, popularity, artists_id, artists_name, danceability, energy, key, loudness, speechiness, acousticness, instrumentalness, liveness, valence, tempo, duration_ms, time_signature, mode) FROM '/opt/spark/resources/data.csv' WITH DELIMITER = ',' AND HEADER = TRUE;\" " % (cassandra_host, cassandra_username, cassandra_password, cassandra_keyspace),
    dag = dag
)

# Bash command to rename the .csv file and move it to the history folder
create_file_history = BashOperator(
    task_id = "create_file_history",
    bash_command = "mkdir -p /opt/spark/resources/history && mv /opt/spark/resources/data.csv /opt/spark/resources/history/%s" % history_filename,
    dag = dag
)

# Dummy operator to add a final step
end = DummyOperator(task_id = "end", dag = dag)

start >> spark_submit >> cassandra_load >> create_file_history >> end