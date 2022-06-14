import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

HOST = config.get('CLUSTER', 'HOST')
DB_NAME = config.get('CLUSTER', 'DB_NAME')
DB_USER = config.get('CLUSTER', 'DB_USER')
DB_PASSWORD = config.get('CLUSTER', 'DB_PASSWORD')
DB_PORT = config.get('CLUSTER', 'DB_PORT')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"  # drop staging_events_table
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"  # drop staging_songs_table
songplay_table_drop = "DROP TABLE IF EXISTS songplays"  # drop songplays table
user_table_drop = "DROP TABLE IF EXISTS users"  # drop users table
song_table_drop = "DROP TABLE IF EXISTS songs"  # drop song_table
artist_table_drop = "DROP TABLE IF EXISTS artists"  # drop artist_table
time_table_drop = "DROP TABLE IF EXISTS time"  # drop time_table

# CREATE TABLES

staging_songs_table_create = (""" CREATE TABLE staging_songs_table (
    staging_song_id     BIGINT IDENTITY(0,1) NOT NULL,
    song_id             VARCHAR(100),
    title               VARCHAR(200),
    duration            FLOAT4,
    year                SMALLINT,
    artist_id           VARCHAR(100),
    artist_name         VARCHAR(200),
    artist_latitude     REAL,
    artist_longitude    REAL,
    artist_location     VARCHAR(200),
    num_songs           INT
    );
""")

staging_events_table_create = (""" CREATE TABLE staging_events_table (
    staging_event_id    BIGINT IDENTITY(0,1) NOT NULL PRIMARY KEY,
    artist              VARCHAR(200),
    auth                VARCHAR(100),
    first_name          VARCHAR(100),
    gender              VARCHAR(100),
    item_in_session     INT,
    last_name           VARCHAR(100),
    length              FLOAT4,
    level               VARCHAR(100),
    location            VARCHAR(100),
    method              VARCHAR(100),
    page                VARCHAR(100),
    registration        FLOAT8,
    session_id          INT,
    song                VARCHAR(200),
    status              INT,
    ts                  BIGINT,
    user_agent          VARCHAR(300),
    user_id             INT
    )
    ;
""")

songplay_table_create = (""" CREATE TABLE songplays (
    songplay_id         BIGINT IDENTITY(0,1) PRIMARY KEY,
    --start_time BIGINT REFERENCES time(start_time) distkey,
    start_time          BIGINT distkey,
    user_id             VARCHAR(100) NOT NULL,
    level               VARCHAR(10),
    song_id             VARCHAR(20) NOT NULL,
    artist_id           VARCHAR(20) NOT NULL,
    session_id          INT,
    location            VARCHAR(256),
    user_agent          VARCHAR(256)
    )
    sortkey(level, start_time);
""")

user_table_create = (""" CREATE TABLE users (
    user_id             INT PRIMARY KEY,
    first_name          VARCHAR(256),
    last_name           VARCHAR(256),
    gender              CHAR(1),
    level               VARCHAR(10) NOT NULL
    )
    diststyle all
    sortkey(level, gender, first_name, last_name);
""")

song_table_create = (""" CREATE TABLE songs (
    song_id             VARCHAR(20) PRIMARY KEY,
    title               VARCHAR(256) NOT NULL,
    artist_id           VARCHAR(20) NOT NULL,
    year                SMALLINT NOT NULL,
    duration            NUMERIC NOT NULL
)
diststyle all
sortkey(year, title, duration);
""")

artist_table_create = ("""CREATE TABLE artists (
    artist_id           VARCHAR(20) PRIMARY KEY,
    name                VARCHAR(256) NOT NULL,
    location            VARCHAR(256),
    latitude            NUMERIC,
    longitude           NUMERIC
)
diststyle all
sortkey(name, location);
""")

time_table_create = ("""CREATE TABLE time (
    start_time          TIMESTAMP PRIMARY KEY distkey, 
    hour                SMALLINT NOT NULL,
    day                 SMALLINT NOT NULL,
    week                SMALLINT NOT NULL,
    month               SMALLINT NOT NULL,
    year                SMALLINT NOT NULL,
    weekday             SMALLINT NOT NULL
)
    sortkey(year, month, day);
""")

# STAGING TABLES

staging_events_copy = (""" COPY staging_events_table (
    artist, auth, first_name, gender, item_in_session, last_name,
    length, level, location, method, page, registration,
    session_id, song, status, ts, user_agent, user_id
)
FROM {} iam_role {} json {} region 'us-west-2';
""").format(config['S3']['log_data'], config['IAM_ROLE']['ARN'], config['S3']['log_jsonpath'])

staging_songs_copy = ("""
copy staging_songs_table (
    song_id, title, duration, year, artist_id, artist_name, artist_latitude, artist_longitude, artist_location, num_songs
)
FROM {} iam_role {} json 'auto' region 'us-west-2';
""").format(config['S3']['song_data'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = (""" INSERT INTO songplays (
        start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT
        st.ts, st.user_id, st.level, s.song_id, s.artist_id, st.session_id, st.location, st.user_agent
    FROM staging_events_table st
    JOIN  staging_songs_table s
    ON st.song = s.title AND st.artist = s.artist_name AND st.length = s.duration
    ON CONFLICT (start_time, user_id, session_id) DO NOTHING;
""")

user_table_insert = (""" INSERT INTO users (
        user_id, first_name, last_name, gender, level)
    SELECT user_id, first_name, last_name, gender, level
    FROM (
        SELECT user_id, first_name, last_name, gender, level,
        ROW_NUMBER() OVER (PARTITION BY user_id
                            ORDER BY first_name, last_name, gender, level) AS rank_user_by_id
        FROM staging_events_table
        WHERE user_id IS NOT NULL
    ) AS ranked
    WHERE ranked.rank_user_by_id = 1
    ON CONFLICT (user_id) DO UPDATE SET level=EXCLUDED.level;
""")

song_table_insert = (""" INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT song_id, title, artist_id, year, duration
FROM (
    SELECT song_id, title, artist_id, year, duration,
    ROW_NUMBER() OVER (PARTITION BY song_id
                        ORDER BY title, artist_id, year, duration) AS rank_song_by_id
    FROM staging_songs_table
    WHERE song_id IS NOT NULL
) AS ranked
WHERE ranked.rank_song_by_id = 1
ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = (""" INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM (
    SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude,
    ROW_NUMBER() OVER (PARTITION BY artist_id
                        ORDER BY artist_name, artist_location, artist_latitude, artist_longitude) AS rank_artist_by_id
    FROM staging_songs_table
    WHERE artist_id IS NOT NULL
) AS ranked
WHERE ranked.rank_artist_by_id = 1
ON CONFLICT (artist_id) DO NOTHING;
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time,
            EXTRACT(HOUR FROM start_time) AS hour,
            EXTRACT(DAY FROM start_time) AS day,
            EXTRACT(WEEK FROM start_time) AS week,
            EXTRACT(MONTH FROM start_time) AS month,
            EXTRACT(YEAR FROM start_time) AS year,
            EXTRACT(DOW FROM start_time) AS weekday
    FROM staging_events_table
    WHERE ts IS NOT NULL
    ON CONFLICT (start_time) DO NOTHING;
""")

staging_row_count = "SELECT COUNT(*) AS count FROM {}"

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                    song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert,
                        time_table_insert]

copy_staging_order = ['staging_events_table', 'staging_songs_table']
count_staging_queries = [staging_row_count.format(table) for table in
                        copy_staging_order]  # list of queries to count staging tables

insert_table_order = ['users', 'songs', 'artists', 'time', 'songplays']

# count_fact_dim_queries = [staging_row_count.format(table) for table in copy_staging_order] + [staging_row_count.format(table) for table in insert_table_order if table != 'songplays' and table != 'time' and table != 'artists' and table != 'songs' and table != 'users' ]

count_fact_dim_queries = [staging_row_count.format(insert_table_order[0]),
                        staging_row_count.format(insert_table_order[1]),
                        staging_row_count.format(insert_table_order[2]),
                        staging_row_count.format(insert_table_order[3]),
                        staging_row_count.format(insert_table_order[4])]
