# Data-Engineer-project
## Master's Degree in Business Intelligence and Big Data in Secure Environments
This project corresponds to the Master's Thesis in Big Data and uses the social network **Twitter** to obtain information about the latest music listened to by users (by searching the #NowPlaying hashtag) and then query the track and artist data involved that **Spotify**, a music-as-a-service based platform, has. The entire process is managed using recognized tools within the field of Big Data.

The project can be downloaded and executed locally or accessed via [Data Engineer Project](http://adrianriesco.com:8000).

> :warning: If you face any kind of problem or the web is running slowly, I encourage you to run the project in your local environment.

> :information_source: For more detail, please refer to the project report located in the [doc folder](https://github.com/AdrianRiesco/Data-Engineer-project/blob/main/doc/memoria.pdf).

## Description
This project implements a **ETL process** to collect data from Twitter. The steps of the process are:
 1. The **Twitter API** is consulted to gather the tweets with the hashtag #NowPlaying. The name of the endpoint queried is _recent search_.
 2. The **tweet** is cleaned up, stopwords and other hashtags are removed, and the remaining text (which usually corresponds to the track name and artist) is isolated.
 3. The **Spotify API** is queried with the previously cleaned up text to collect the identified track information. The names of the endpoints queried are _search for item_ and _get tracks' audio features_.
 4. The data is formatted and stored in a **.csv** file.
 5. The data is uploaded to **Cassandra** and the .csv is stored as a history file.
 6. The data served from Cassandra is requested by the **back-end** and served on the **front-end**.
 7. The data is displayed to the **user** on the front-end.

![alt text](https://github.com/AdrianRiesco/Data-Engineer-project/blob/main/flask/static/images/flowchart.gif "Flow chart")

The project has been built using **Docker** and **Docker Compose** to run the following containers:
 - **Apache Airflow** containers configured for flow orchestration: webserver, scheduler, worker, init, triggerer, redis, postgres, client, and flower. Airflow setup required a custom image with the following packages installed via PyPI as additional requirements: "apache-airflow-providers-apache-spark", "requests", "pandas", "cqlsh". In addition, in the Airflow Dockerfile, the Java SDK 11 was installed and the JAVA_HOME variable set. The image used as base image is the official Airflow image (version 2.3.0) found on DockerHub (https://hub.docker.com/r/apache/airflow), as well as the Docker Compose base file (https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html\#docker-compose-yaml).
 - **Apache Spark** containers were configured for data processing: master and three workers. Spark setup required a custom image with the following packages installed via PyPI as additional requirements: "requests", "pandas", "cqlsh". The image used as base image was the Bitnami Spark image (version 3.1.2) obtained from DocherHub (https://hub.docker.com/r/bitnami/spark).
 - An **Apache Cassandra** container was configured for data storage, using an additional container to set up the database configuration. The image used was the official Cassandra (version 4.0) image found on DockerHub (https://hub.docker.com/_/cassandra) and no additional requirements were needed.
 - A **Linux** container was configured for the web application. The container required a custom image with the following packages installed via PyPI as additional requirements: flask (version 2.1.2), cassandra-driver (version 3.25.0), flask-cqlalchemy (version 2.0.0), redis, Cmake, cryptography. The image used as base image was the official Python image (version "3.8-slim-buster") found on DockerHub (https://hub.docker.com/_/python).

## Prerequisites
 - **Git**
 - **Docker** v20.10.12.
 - **Docker-compose** v1.29.2.
 - **Python** v3.8.10.
 - Twitter and Spotify APIs **developer keys** added in a file **.env** following the format of the .env.example file and located in the same folder.

 > The correct operation of the project with different versions is not guaranteed.

## Usage
**Clone** the project and execute **docker-compose up** in the command console:
```
$ git clone https://github.com/AdrianRiesco/Data-Engineer-project.git
...
$ cd Data-Engineer-project/docker
$ sudo docker-compose up
```

Once the project is deployed, three visual interfaces can be accessed that can help the user to better understand the process:
 1. **Apache Airflow user interface.** It is accessible through port 8080 (http://localhost:8080, user "airflow", password "airflow") and allows access, among other things, to the Airflow configuration and the list of configured DAGs, being able to observe their instances and obtain metrics such as execution times.
    <kbd>![alt text](https://github.com/AdrianRiesco/Data-Engineer-project/blob/main/doc/img/airflow-ui.jpg "Airflow UI")</kbd>
 2. **Apache Spark user interface.** It is accessible through port 8181 (http://localhost:8181) and allows to observe the master node and the three workers, as well as their last tasks performed.
    <kbd>![alt text](https://github.com/AdrianRiesco/Data-Engineer-project/blob/main/doc/img/spark-ui.jpg "Spark UI")</kbd>
 3. **Front-end user interface.** It is accessible through port 8000 (http://localhost:8000) and contains three views:
    1. **Home.** It shows a brief introduction of the project and the same animated gif that can be viewed in this page to illustrate the implemented data flow.
       <kbd>![alt text](https://github.com/AdrianRiesco/Data-Engineer-project/blob/main/doc/img/front-home.jpg "Home view")</kbd>
    2. **Data.** It displays a table with all the data extracted. The table has been made with Datatables and allows the user to search and sort the data, as well as hide and show columns at will.
       <kbd>![alt text](https://github.com/AdrianRiesco/Data-Engineer-project/blob/main/doc/img/front-data.jpg "Data view")</kbd>
    3. **Visuals.** It displays a chart with two data selectors, a multi-select to add data as a bar and a single-select to add data as a line. A script was added that does not allow both selectors to have the same column in their data, so when a column that is present in one of them is selected in the other, it is removed from the previous one. It also has a selector to choose the amount of data to display (values of 5, 10, 15 and 20, to avoid saturating the graph), as well as an order selector that allows the user to sort by any of the columns in ascending or descending order.
       <kbd>![alt text](https://github.com/AdrianRiesco/Data-Engineer-project/blob/main/doc/img/front-visuals.jpg "Visuals view")</kbd>
    

After the services are running, access to the **Airflow UI** and activate the DAG "**spark_main**". After all the tasks are executed correctly, the data will be displayed in the web application views. If not paused, the DAG will be executed every 30 minutes.

<kbd>![alt text](https://github.com/AdrianRiesco/Data-Engineer-project/blob/main/doc/img/start-dag.jpg "Start DAG in Airflow UI")</kbd>

To **stop** the project, run the following command within the docker folder:
```
$ sudo docker-compose down
```

To completely **reset** the project, run the following command within the docker folder:
```
$ sudo docker-compose down -v
```

 > Adrian Riesco Valbuena.