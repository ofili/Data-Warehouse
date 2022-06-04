import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table" # drop staging_events_table
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table" # drop staging_songs_table	
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table" # drop songplay_table
user_table_drop = "DROP TABLE IF EXISTS user_table" # drop user_table
song_table_drop = "DROP TABLE IF EXISTS song_table" # drop song_table
artist_table_drop = "DROP TABLE IF EXISTS artist_table" # drop artist_table
time_table_drop = "DROP TABLE IF EXISTS time_table" # drop time_table

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events_table (
    stagingEventId bigint IDENTITY(0,1) PRIMARY KEY,
    artist varchar(256),
    auth varchar(256),
    firstName varchar(256),
    gender char(1),
    itemInSession smallint,
    lastName varchar(256),
    length numeric,
    location varchar(256),
    method varchar(20),
    page varchar(256),
    registration numeric,
    sessionId smallint,
    song varchar(256),
    status smallint,
    ts bigint,
    userAgent varchar(256),
    userId smallint
    );
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs_table (
    staging_song_id bigint IDENTITY(0,1) PRIMARY KEY,
    num_songs int NOT NULL,
    artist_id varchar(256) NOT NULL,
    artist_latitude numeric,
    artist_longitude numeric,
    artist_location varchar(256),
    artist_name varchar(256) NOT NULL,
    song_id varchar(256) NOT NULL,
    title varchar(256) NOT NULL,
    duration numeric NOT NULL,
    year int NOT NULL
    );
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id bigint IDENTITY(0,1) PRIMARY KEY,
    start_time bigint REFERENCES time(start_time) distkey,
    user_id int NOT NULL REFERENCES users(user_id),
    level varchar(10),
    song_id varchar(20) NOT NULL REFERENCES songs(song_id),
    artist_id varchar(20) REFERENCES artists(artist_id),
    session_id int,
    location varchar(256),
    user_agent varchar(256)
    )
    sortkey(level, start_time);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id int PRIMARY KEY,
    first_name varchar(256),
    last_name varchar(256),
    gender char(1),
    level varchar(10) NOT NULL
    )
    diststyle all
    sortkey(level, gender, first_name, last_name);
""")

song_table_create = ("""
CREATE TABLE songs (
    song_id varchar(20) PRIMARY KEY,
    title varchar(256) NOT NULL,
    artist_id varchar(20) NOT NULL,
    year smallint NOT NULL,
    duration numeric NOT NULL
)
diststyle all
sortkey(year, title, duration);
""")

artist_table_create = ("""
CREATE TABLE artists (
    artist_id varchar(20) PRIMARY KEY,
    name varchar(256) NOT NULL,
    location varchar(256),
    latitude numeric,
    longitude numeric
)
diststyle all
sortkey(name, location);
""")

time_table_create = ("""
CREATE TABLE time (
    start_time timestamp PRIMARY KEY distkey,
    hour smallint NOT NULL,
    day smallint NOT NULL,
    week smallint NOT NULL,
    month smallint NOT NULL,
    year smallint NOT NULL,
    weekday smallint NOT NULL
)
sortkey(year, month, day);
""")

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
