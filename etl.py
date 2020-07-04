"""Etl project to create datalake using spark"""
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """Creates the spark connection"""
    spark = SparkSession.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0").getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """Process song and artist data"""
    song_data = input_data + "song_data/*/*/*"
    song_data = "s3a://data-cap/song_data/*/*/*"
    
    print("Songs data processing")
    df = spark.read.format("json").load(song_data)
    df = df.withColumn("year", df["year"].cast(IntegerType()))
    df = df.withColumn("title", F.trim(F.col("title")))
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")

    print("storing songs table as parquet files")
    songs_table.write.partitionBy("artist_id").parquet(output_data + "songs_table", mode='overwrite')
    songs_table.show()
    
    artists_table = df.select("artist_id", "artist_name", "year", "duration")
    print("storing artists table as parquet files")
    artists_table.write.parquet(output_data + "artists_table", mode='overwrite')
    artists_table.show()
    

def process_log_data(spark, input_data, output_data):
    """Processs logs data and create fact table and save it to s3"""
    log_data = input_data + "log_data/*/*/*"
    log_data = "s3a://data-cap/log_data/*/*/*"
    
    df = spark.read.format("json").load(log_data)
    df = df.where(df.page == "NextSong")

    def get_ts(x):
        """udf to convert ts to datetime"""
        return datetime.fromtimestamp(x/1000)
    
    get_time_stamp = udf(get_ts, TimestampType())
    df = df.withColumn('start_time', get_time_stamp('ts'))
    df = df.withColumn("songplay_id", F.monotonically_increasing_id())
    df.createOrReplaceTempView("log_data")
    df.show()

    # create uses table and write to s3
    users_table = df.select(
        F.col("userid").alias("user_id"),
        F.col("firstName").alias("first_name"),
        F.col("lastName").alias("last_name"),
        F.col("gender").alias("gender"),
        F.col("level").alias("level")
    ).distinct()
    users_table.write.parquet(output_data + "users_table", mode='overwrite')
    users_table.show()

    # Create time_table
    time_table = df.select("start_time",
                           F.hour("start_time").alias('hour'),
                           F.dayofmonth("start_time").alias('day'),
                           F.weekofyear("start_time").alias('week'),
                           F.month("start_time").alias('month'),
                           F.year("start_time").alias('year'),
                           F.date_format("start_time","u").alias('weekday')
                          ).distinct()
    # Write time_table to s3
    time_table.write.partitionBy("year", "month").parquet(output_data + "time_table", mode='overwrite')

    # read songs data from s3
    song_df = spark.read.parquet(output_data + "songs_table")
    song_df.createOrReplaceTempView("songs_table")
    song_df.show()

    # read artists data from s3
    artist_df = spark.read.parquet(output_data + "artists_table")
    artist_df.createOrReplaceTempView("artists_table")
    artist_df.show()

    # Create a time_table view for exploratory
    time_table.createOrReplaceTempView("time_table")
    time_table.show()

    # Create the fact table by joining logs, songs and artist tables
    songplays_table = spark.sql(""" SELECT log.start_time,
                                       log.userid,
                                       log.level,
                                       art.artist_id,
                                       song.song_id,
                                       log.sessionid,
                                       log.location,
                                       log.useragent
                                       FROM log_data log JOIN artists_table art ON (log.artist = art.artist_name)
                                       JOIN songs_table song ON (song.artist_id = art.artist_id)""")
    # songplays_table.write.partitionBy("userid").parquet(output_data + "songplays_table", mode='overwrite')
    songplays_table.show()
    print(f"number of records in songplays_table {songplays_table.count()}")


# Run the pipe line
spark = create_spark_session()
input_data = "s3a://udacity-dend/"
output_data = "s3a://data-cap/spark_proj_out/"

# process_song_data(spark, input_data, output_data)
process_log_data(spark, input_data, output_data) 
