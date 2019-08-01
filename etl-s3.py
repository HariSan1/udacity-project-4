import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import types as t
from pyspark.sql.functions import udf, col

config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))
#read in parameters from dl.cfg file for AWS authentication

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session_docstring():
    """
    Create spark session to create database and process data
    """
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data_docstring(spark, input_data, output_data):
    """
    Read song data files from input_data location
    Select columns and create tables for songs, artists tables with spark.sql and populate with data read
    Write data to files using parquet method
    
    keyword arguments in process_song_data:
        spark:         spark session name
        input_data:    location of songs data  - e.g., workspace, S3, pc local
        output_data:   location of output data - e.g., workspace, S3, pc local
    """

def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    #song_data = os.path.join(input_data, "song_data/*.json")
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    df.createOrReplaceTempView("songs_table_df")
    songs_table = spark.sql("""
        SELECT song_id, title, artist_id, year, duration
        FROM songs_table_df
        ORDER BY song_id
    """)
    print("Songs table schema: ")
    songs_table.printSchema()
    
    # write songs table to parquet files partitioned by year and artist
    now = datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f')
    songs_table_path = output_data + "songs_table.parquet" + "_" + now
    songs_table.write.mode("overwrite").partitionBy("year","artist_id").parquet(songs_table_path)

    # extract columns to create artists table
    df.createOrReplaceTempView("artists_table_df")
    artists_table = spark.sql("""
        SELECT  artist_id        AS artist_id,
                artist_name      AS name,
                artist_location  AS location,
                artist_latitude  AS latitude,
                artist_longitude AS longitude
        FROM artists_table_df
        ORDER by artist_id desc
    """)
    
    print("artists table schema: ")
    artists_table.printSchema()
    
    # write artists table to parquet files
    now = datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f')
    artists_table_path = output_data + "artists_table.parquet" + "_" + now
    artists_table.write.mode("overwrite").parquet(artists_table_path)
    
    return songs_table, artists_table

def process_song_data_docstring(spark, input_data, output_data):
    """
    Read log data files from input_data location
    Select columns and create tables for users, time, songplays tables with spark.sql and populate with data read
    Get time-stamp and date-time information with user-defined-functions (udf)
    Extract columns from song and log data to create songplays table
    Join artist and song data and get song and artist names
    Write data to files using parquet method
    
    keyword arguments in process_song_data:
        spark:         spark session name
        input_data:    location of songs data  - e.g., workspace, S3, pc local
        output_data:   location of output data - e.g., workspace, S3, pc local
    """

def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    #log_data = os.path.join(input_data, "log_data/*.json")
    log_data = '{input_data}/log_data/'
    
    # read log data file
    df_log_data = spark.read.json(log_data)
    
    # filter by actions for song plays
    df_log_data_filtered = df_log_data.filter(df_log_data.page == 'NextSong') 

    # extract columns for users table    
    df_log_data_filtered.createOrReplaceTempView("users_table_df")
    users_table = spark.sql("""
        SELECT DISTINCT userId    AS user_id,
                        firstName AS first_name,
                        lastName  AS last_name,
                        gender,
                        level
        FROM users_table_df
        ORDER BY last_name
    """)
    print("users table schema and sample data:")
    users_table.printSchema()
    users_table.show(5)
 
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from datetime import datetime

    # write users table to parquet files
    now = datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f')
    users_table_path = output_data + "users_table.parquet" + "_" + now
    users_table.write.mode("overwrite").parquet(users_table_path)

    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from datetime import datetime
    get_timestamp = F.udf(lambda x: datetime.fromtimestamp( (x/1000.0) ), T.TimestampType()) 

    df_log_data_filtered = df_log_data_filtered.withColumn("timestamp", get_timestamp("ts"))
    df_log_data_filtered.printSchema()
    df_log_data_filtered.show(5)

    # create datetime column from original timestamp column
    @udf(t.StringType())
    def get_datetime(ts):
        return datetime.fromtimestamp(ts / 1000.0).strftime('%Y-%m-%d %H:%M%S')
    df_log_filtered = df_log_data_filtered.withColumn("datetime", get_datetime("ts"))

    # extract columns, including datetime, to create time table
    df_log_filtered.createOrReplaceTempView("time_table_df")
    time_table =  spark.sql("""
            SELECT DISTINCT datetime                AS start_time,
                            hour(timestamp)         AS hour,
                            day(timestamp)          AS day,
                            weekofyear(timestamp)   AS week,
                            month(timestamp)        AS month,
                            year(timestamp)         AS year,
                            dayofweek(timestamp)    AS weekday
            FROM time_table_df
            ORDER BY start_time
        """)
    time_table.printSchema()
    time_table.show(5)

    # write time table to parquet files partitioned by year and month
    now = datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f')
    time_table_path = output_data + "time_table.parquet" + "_" + now
    time_table.write.mode("overwrite").partitionBy("year","month").parquet(time_table_path)

    # read in song data to use for songplays table

    song_data = os.path.join(input_data, "song_data/*.json")
    df_song = spark.read.json(song_data)
    df_joined = df_log_filtered.join(df_song, (df_log_filtered.artist == df_song.artist_name) & \
                                     (df_log_filtered.song == df_song.title))
    df_joined.printSchema()
    df_joined.show(5)

    # extract columns from joined song and log datasets to create songplays table 
    df_joined = df_joined.withColumn("songplay_id", monotonically_increasing_id())
    df_joined.createOrReplaceTempView("songplays_table_df")

    songplays_table = spark.sql("""
            SELECT songplay_id   AS songplay_id,
                   timestamp     AS start_time,
                   userId        AS user_id,
                   level         AS level,
                   song_id       AS song_id,
                   artist_id     AS artist_id,
                   sessionId     AS session_id,
                   location      AS location,
                   userAgent     AS user_agent
            FROM songplays_table_df
            ORDER BY (user_id, session_id)
        """)

    songplays_table.printSchema()
    songplays_table.show(5)

    # write songplays table to parquet files partitioned by year and month
    now = datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f')
    songplays_table_path = output_data + "songplays_table.parquet" + "_" + now
    songplays_table.write.mode("overwrite").partitionBy("year", "month")

    return users_table, time_table, songplays_table

def main_docstring():
    """
    Main function calls the work process functions process_song_data and process_log_data
    Global names:
    spark:   spark session name
    input_data: where existing song and log data are stored for the purposes of this program run (e.g., Udacity workspace, S3, pc local folder etc.)
    output_data: where program output will be stored (e.g., Udacity workspace, S3, pc local folder etc.)
    """

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://hs-output_data/"
    #input_data  = "data/"
    #output_data = "data/output_data/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
