### Purpose of the database and ETL pipeline
Purpose of this ETL pipeline is to automate data processing and analysis steps.

### Raw JSON data structures
* **log_data**: log_data contains data about what users have done (columns: event_id, artist, auth, firstName, gender, itemInSession, lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userAgent, userId)
* **song_data**: song_data contains data about songs and artists (columns: num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year)

### Fact Table
* **songplays**: song play data together with user, artist, and song info (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)

### Dimension Tables
* **users**: user info (columns: user_id, first_name, last_name, gender, level)
* **songs**: song info (columns: song_id, title, artist_id, year, duration)
* **artists**: artist info (columns: artist_id, name, location, latitude, longitude)
* **time**: detailed time info about song plays (columns: start_time, hour, day, week, month, year, weekday)

### Structure
* `udac_example_dag.py` contains the tasks and dependencies of the DAG.
* `create_tables.sql` contains the SQL queries used to create all the required tables in Redshift.
* `sql_queries.py` contains the SQL queries used in the ETL process.
* `stage_redshift.py` contains `StageToRedshiftOperator`, which copies JSON data from S3 in the Redshift.
* `load_dimension.py` contains `LoadDimensionOperator`, which loads a dimension table from s3.
* `load_fact.py` contains `LoadFactOperator`, which loads a fact table from s3.
* `data_quality.py` contains `DataQualityOperator`, which runs a data quality check by passing an SQL query and expected result as arguments, failing if the results don't match.

### HOWTO use
**Project has one DAG:**
* **etl_dag**: This DAG uses data in s3:/udacity-dend/song_data and s3:/udacity-dend/log_data, processes it, and inserts the processed data into AWS Redshift DB.