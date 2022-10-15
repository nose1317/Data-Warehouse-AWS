# Data-Warehouse-AWS
Project for Udacity Data Engineering Nanodegree - Data Warehouse

## Introduction
helping **Sparkify** to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to. 

## Project Datasets

**Song data:** `s3://udacity-dend/song_data`

**Log data:** `s3://udacity-dend/log_data`
**Log data json path:** `s3://udacity-dend/log_json_path.json`

## Schema for Song Play Analysis
Using the song and event datasets, you'll need to create a star schema optimized for queries on song play analysis. This includes the following tables.

#### Fact Table
songplays - records in event data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#### Dimension Tables
users - users in the app
user_id, first_name, last_name, gender, level
songs - songs in music database
song_id, title, artist_id, year, duration
artists - artists in music database
artist_id, name, location, lattitude, longitude
time - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday

### Configurations and Setup

Create an IAM Role that makes Redshift able to access S3 bucket (ReadOnly) and get **ROLE_ARN**
Create a RedShift Cluster and get the **Host address** 
fill the config file.

### Project Template

`create_table.py` create your fact and dimension tables for the star schema in Redshift.
`etl.py` load data from S3 into staging tables on `Redshift` and then process that data into your `analytics` tables on Redshift.
`sql_queries.py` define you SQL statements, which will be imported into the two other files above.

### Run
- Complete `dwh.cfg`
- Create tables by running `create_tables.py`.
- Execute ETL process by running `etl.py`.
