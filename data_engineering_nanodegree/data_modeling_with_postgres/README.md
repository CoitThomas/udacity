#### Udacity Data Engineering Nanodegree Program
#### Project: Data Modeling with Postgres


### Project Overview

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a Postgres database with tables designed to optimize queries on song play analysis. This project aims to create a database schema and ETL pipeline for their analysis.


### Project Repository Files

The `song_data` and `log_data` datasets are located in their respective directories nested under the `/data` directory.

* `create_tables.py` drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.

* `etl.ipynb` reads and processes a single file from `song_data` and `log_data` and loads the data into your tables. This notebook contains detailed instructions on the ETL process for each of the tables.

* `etl.py` reads and processes files from `song_data` and `log_data` and loads them into your tables. You can fill this out based on your work in the ETL notebook.

* `sql_queries.py` contains all your sql queries, and is imported into the last three files above.

* `test.ipynb` displays the first few rows of each table to let you check your database.


### Database Schema Design

![Sparkify database star schema](https://github.com/CoitThomas/udacity/blob/master/data_engineering_nanodegree/data_modeling_with_postgres/images/db_star_schema.png)

A star schema was chosen for the implementation of the project. It's a simple and popular schema which provides simpler queries and faster aggregrations allowing Sparkify to optimize for queries which assist in song play analysis. The schema includes the following tables:

##### Fact Table
**songplays** - records in log data associated with song plays i.e. records with page `NextSong`
    * *songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

##### Dimension Tables
1. **users** - users in the app
    * *user_id, first_name, last_name, gender, level*
2. **songs** - songs in music database
    * *song_id, title, artist_id, year, duration*
3. **artists** - artists in music database
    * *artist_id, name, location, latitude, longitude*
4. **time** - timestamps of records in songplays broken down into specific units
    * *start_time, hour, day, week, month, year, weekday*


### ETL Pipeline Process

The ETL process starts by connecting to the Sparkify database. It then scours the `/data` directory for all of the `song_data` and `log_data` json file paths it can find. Upon collecting those file paths, the etl process then iterates over each one and reads the json in as a pandas dataframe. From each song_data dataframe, an artist record and song record are extracted and inserted into the `songs` table and `artists` table respectively. From each log_data dataframe, a time record, user record, and songplay record are extracted and inserted into the `time` table, `users` table, and `songplays` table respectively.


### Running the Project

    $ python3 create_tables.py
    $ python3 etl.py
