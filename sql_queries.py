import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events(
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession INT,
    lastName VARCHAR,
    length DOUBLE PRECISION,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR ,
    page VARCHAR,
    registration VARCHAR,
    sessionid INT,
    song VARCHAR,
    status INT,
    ts VARCHAR,
    userAgent VARCHAR,
    userId INT
    )
"""
)

staging_songs_table_create = ("""

    CREATE TABLE IF NOT EXISTS "staging_songs" (
    num_songs INTEGER,
    artist_id VARCHAR,
    artist_latitude VARCHAR,
    artist_longitude VARCHAR,
    artist_location VARCHAR ,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration NUMERIC NOT NULL,
    year integer
);
"""
)
# start_time varrchar or timestamp
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id bigint identity(0, 1) PRIMARY KEY, 
        start_time varchar, 
        user_id int NOT NULL, 
        level varchar(255), 
        song_id varchar(255), 
        artist_id varchar(255), 
        session_id int NOT NULL, 
        location varchar(255), 
        user_agent varchar(255)
        )
""");

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id varchar PRIMARY KEY, 
        first_name varchar(255), 
        last_name varchar(255), 
        gender varchar(255), 
        level varchar(255)
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id varchar PRIMARY KEY, 
        title varchar(255), 
        artist_id varchar(255) NOT NULL, 
        year int, 
        duration FLOAT NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id varchar(255) PRIMARY KEY, 
        name varchar(255), 
        location varchar(255), 
        latitude varchar(255), 
        longitude varchar(255)
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time_table(
        start_time timestamp PRIMARY KEY, 
        hour varchar(255), 
        day varchar(255), 
        week varchar(255), 
        month varchar(255), 
        year varchar(255), 
        weekday varchar(255)
    );
""")

# STAGING TABLES

staging_events_copy = ("""

    copy staging_events from {}
    credentials 'aws_iam_role={}'
    format as json {} 
    compupdate off region 'us-west-2';
""").format(config.get('S3', 'LOG_DATA'),
           config.get('IAM_ROLE', 'ARN'),
           config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    compupdate off region 'us-west-2'
    json 'auto';
""").format(config.get('S3', 'SONG_DATA'),
           config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT event.ts, event.userId, event.level, song.song_id, song.artist_id, event.sessionId, event.location, event.userAgent 
FROM staging_events event
INNER JOIN staging_songs song ON event.song = song.title AND event.artist = song.artist_name;
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT DISTINCT event.userId, event.firstName, event.lastName, event.gender, event.level
FROM staging_events event
where event.userId IS NOT NULL

""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration) 
SELECT song.song_id, song.title, song.artist_id, song.year, song.duration
FROM staging_songs song
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, latitude, longitude) 
SELECT song.artist_id, song.artist_name, song.artist_longitude, song.artist_latitude, song.artist_location
FROM staging_songs song
""")

time_table_insert = ("""
INSERT INTO time_table(start_time, hour, day, week, month, year, weekday) 
SELECT DISTINCT timestamp 'epoch' + CAST(event.ts AS BIGINT)/1000 * interval '1 second' as start_time, 
EXTRACT(hour FROM timestamp 'epoch' + CAST(event.ts AS BIGINT)/1000 * interval '1 second') as hour, 
EXTRACT(day FROM timestamp 'epoch' + CAST(event.ts AS BIGINT)/1000 * interval '1 second') as day, 
EXTRACT(week FROM timestamp 'epoch' + CAST(event.ts AS BIGINT)/1000 * interval '1 second') as week, 
EXTRACT(month FROM timestamp 'epoch' + CAST(event.ts AS BIGINT)/1000 * interval '1 second') as month, 
EXTRACT(year FROM timestamp 'epoch' + CAST(event.ts AS BIGINT)/1000 * interval '1 second') as year, 
EXTRACT(weekday FROM timestamp 'epoch' + CAST(event.ts AS BIGINT)/1000 * interval '1 second') as weekday
FROM staging_events event
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
print("----- create table done ")
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
print("----- drop tables done ")
copy_table_queries = [staging_events_copy, staging_songs_copy]
print("----- copy tables done ")
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
print("----- insert data done ")