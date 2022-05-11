# Import libraries
from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.dummy_operator import DummyOperator

# Operators are objects that encapsulate the job
# Pending: Check conn_id

# *** Parameters
spark_master = "spark://spark:7077"
spark_app_name = "Spark Data Extraction"
file_path = "/usr/local/spark/resources/data/airflow.cfg"

# *** DAG
now = datetime.now()

dag = DAG(
    dag_id = "spark_main", 
    description = "This DAG runs the principal Pyspark app to extract information from Twitter and Spotify",
    default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes = 1)},
    start_date = datetime(2022, 1, 1), # Could be changed for: datetime(now.year, now.month, now.day)
    schedule_interval = timedelta(minutes = 30),
    catchup = False
)

# *** Spark Submit Operator
spark_submit = SparkSubmitOperator(
    task_id = "spark_job",
    application = "/usr/local/spark/app/hello-world.py",
    name = spark_app_name,
    conn_id = "spark_default",
    verbose = 1,
    conf = {"spark.master":spark_master},
    application_args = [file_path],
    dag = dag
)

start = DummyOperator(task_id="start", dag=dag)

cassandra_load = DummyOperator(task_id="cassandra_load", dag=dag)

end = DummyOperator(task_id="end", dag=dag)

start >> spark_submit >> cassandra_load >> end
