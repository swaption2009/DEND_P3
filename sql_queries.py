import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (artist VARCHAR, 
                                           auth VARCHAR,
                                           first_name VARCHAR,
                                           gender VARCHAR,
                                           item_in_session INT,
                                           last_name VARCHAR,
                                           length NUMERIC,
                                           level VARCHAR,
                                           location VARCHAR,
                                           method VARCHAR,
                                           page VARCHAR,
                                           registration NUMERIC,
                                           session_id INT,
                                           song VARCHAR,
                                           status INT,
                                           ts BIGINT,
                                           user_agent VARCHAR,
                                           user_id INT);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (num_songs INT,
                                          artist_id VARCHAR,
                                          artist_name VARCHAR,
                                          artist_latitude FLOAT,
                                          artist_longitude FLOAT,
                                          artist_location VARCHAR,
                                          song_id VARCHAR,
                                          title VARCHAR,
                                          duration NUMERIC,
                                          year INT);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (songplay_id INT IDENTITY(0,1) PRIMARY KEY,
                                      start_time TIMESTAMP,
                                      user_id INT,
                                      level VARCHAR,
                                      song_id VARCHAR,
                                      artist_id VARCHAR,
                                      session_id INT,
                                      location VARCHAR,
                                      user_agent VARCHAR);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (user_id INT PRIMARY KEY,
                                  first_name VARCHAR,
                                  last_name VARCHAR,
                                  gender VARCHAR,
                                  level VARCHAR);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (song_id INT PRIMARY KEY,
                                  title VARCHAR,
                                  artist_id VARCHAR,
                                  year INTEGER,
                                  duration FLOAT);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (artist_id INT PRIMARY KEY,
                                    artist_name VARCHAR,
                                    artist_location VARCHAR,
                                    artist_latitude FLOAT,
                                    artist_longitude FLOAT);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (timestamp TIMESTAMP PRIMARY KEY,
                                 hour INT,
                                 day INT,
                                 weekofyear INT,
                                 month VARCHAR,
                                 year INT,
                                 isoweekday VARCHAR);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events 
FROM {}
iam_role '{}'
region 'us-west-2'
JSON {};
""").format(config.get('S3', 'LOG_DATA'),
            config.get('IAM_ROLE', 'ARN'),
            config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
COPY staging_songs 
FROM {}
iam_role '{}'
region 'us-west-2'
JSON 'auto';
""").format(config.get('S3', 'SONG_DATA'),
            config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, 
                       user_id, 
                       level, 
                       song_id, 
                       artist_id, 
                       session_id, 
                       location, 
                       user_agent)
SELECT DISTINCT TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second', 
                e.user_id, 
                e.level, s.song_id, 
                s.artist_id, 
                e.session_id, 
                e.location, 
                e.user_agent
FROM staging_events AS e
JOIN staging_songs as s
ON e.song = s.title;
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT user_id, first_name, last_name, gender, level
FROM staging_events;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id,
                     artist_name, 
                     artist_location, 
                     artist_latitude, 
                     artist_longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs;
""")

time_table_insert = ("""
INSERT INTO time (timestamp, hour, day, weekofyear, month, year, isoweekday)
SELECT DISTINCT start_time, 
       extract(hour from start_time), 
       extract(day from start_time), 
       extract(week from start_time),
       extract(month from start_time),
       extract(year from start_time),
       extract(dayofweek from start_time)
FROM songplays;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
