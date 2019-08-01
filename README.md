PROJECT 4 - Data Lake
This project is to take big data from a music streaming company, Sparkify, and process the data with Apache Spark and on AWS S3 data storage and write output files.  
Project extracts data in JSON format from AWS S3 cloud storage, processes them with Spark, and writes output data to S3.
The files consist of:
* Song files - s3://udacity-dend/song_data: static data about artists and songs 
* Log files - s3://udacity-dend/log_data: event data on songs listened to by users

# Database Design
## Sparkify database is a star design schema
### - 1 Fact table with business data that is considered critical
### - Dimension tables with supporting data
#### The main criteria for the business is to find out which songs users are listening to, and related logistics. So the Songplays table is the fact table, and the supporting information - songs, artists, users and time - are the dimension tables. This is because the songplays table answers the main business question - "which songs are listened to, by which users and when and where?"
## Fact Table
* songplays - records in log data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
## Dimension Tables
* users - users in the app
** user_id, first_name, last_name, gender, level
* songs - songs in music database
** song_id, title, artist_id, year, duration
* artists - artists in music database
** artist_id, name, location, lattitude, longitude
* time - timestamps of records in songplays broken down into specific units
** start_time, hour, day, week, month, year, weekday

# Objectives for Database and ETL
* Data is stored in AWS S3 in JSON files
* Read the data, create tables and process in Spark
* Output the data in parquet files into S3 or other location as desired
* Spark is ideal for handling big data that need fast distributed processing
* Cloud storage is optimized for large data, scalability, and easy access.

# Files
### - etl.py - reads, processes and writes to local Udacity workspace (can also be used to read to and write from AWS S3, but takes very long time).
### - python_test_etl.ipynb - test run for etl.py using notebook.
### - dl.cfg - configuration file with AWS login parameters and input, output parameters.
### data folders
### data/log_data - contains input data for event logs
### data/song_data - contains input data for songs
### output_data - output parquet files written by the program
### data/zip_files - contains zip files for songs and logs that were unzipped into the log_data and song_data folders described above.

# Usage
### To run etl.py - From Terminal, type: python3 etl.py
