import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table" # drop staging_events_table
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table" # drop staging_songs_table	
songplay_table_drop = "DROP TABLE IF EXISTS songplays" # drop songplays table
user_table_drop = "DROP TABLE IF EXISTS users" # drop user_table
song_table_drop = "DROP TABLE IF EXISTS songs" # drop song_table
artist_table_drop = "DROP TABLE IF EXISTS artists" # drop artist_table
time_table_drop = "DROP TABLE IF EXISTS time" # drop time_table

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
COPY staging_events_table (
    artist, auth, firstName, gender, itemInSession, lastName,
    length, level, location, method, page, registration,
    sessionId, song, status, ts, userAgent, userId
)
FROM {} iam_role {} json {} region 'us-west-2';
""").format(config['S3']['log_data'], config['IAM_ROLE']['arn'], config['S3']['log_jsonpath'])

staging_songs_copy = ("""
copy staging_songs_table
FROM {} iam_role {} json 'auto' region 'us-west-2';
""").format(config['S3']['song_data'], config['IAM_ROLE']['arn'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
    start_time, user_id, level, song_id, artist_id, session_id,
    location, user_agent)
SELECT
    st.ts, st.userId, st.level, ss.song_id, ss.artist_id, st.sessionId, st.location, st.userAgent
FROM staging_events_table st
JOIN (
    SELECT s.song_id, s.artist_id, s.title AS song, a.name AS artist, s.duration 
    FROM songs s
    JOIN artists a ON s.artist_id = a.artist_id
) sa 
ON st.song = sa.song AND st.artist = sa.artist AND st.length = sa.duration
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT userId, firstName, lastName, gender, level
FROM (
    SELECT userId, firstName, lastName, gender, level,
    ROW_NUMBER() OVER (PARTITION BY userId
                        ORDER BY firstName, lastName, gender, level) AS rank_user_by_id
    FROM staging_events_table
    WHERE userId IS NOT NULL
) AS ranked
WHERE ranked.rank_user_by_id = 1;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT song_id, title, artist_id, year, duration
FROM (
    SELECT song_id, title, artist_id, year, duration,
    ROW_NUMBER() OVER (PARTITION BY song_id
                        ORDER BY title, artist_id, year, duration) AS rank_song_by_id
    FROM staging_songs_table
    WHERE song_id IS NOT NULL
) AS ranked
WHERE ranked.rank_song_by_id = 1;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT artist_id, name, location, latitude, longitude
FROM (
    SELECT artist_id, name, location, latitude, longitude,
    ROW_NUMBER() OVER (PARTITION BY artist_id
                        ORDER BY name, location, latitude, longitude) AS rank_artist_by_id
    FROM staging_songs_table
    WHERE artist_id IS NOT NULL
) AS ranked
WHERE ranked.rank_artist_by_id = 1;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT 'epoch'::timestamp + ts/1000 * interval '1 second' AS start_time, 
        extract(hour from ts) AS hour, 
        extract(day from ts) AS day, 
        extract(week from ts) AS week, 
        extract(month from ts) AS month, 
        extract(year from ts) AS year, 
        extract(weekday from ts) AS weekday
FROM staging_events_table
WHERE ts IS NOT NULL; 
""")

staging_row_count = "SELECT COUNT(*) AS count FROM {}"

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

copy_staging_order = ['staging_events_table', 'staging_songs_table']
count_staging_queries = [staging_row_count.format(table) for table in copy_staging_order if table != staging_events_table_create and table != staging_songs_table_create]

insert_table_order = ['users', 'songs', 'artists', 'time', 'songplays']

count_fact_dim_queries = [staging_row_count.format(table) for table in copy_staging_order] + [staging_row_count.format(table) for table in insert_table_order if table != 'songplays' and table != 'time' and table != 'artists' and table != 'songs' and table != 'users' ]
