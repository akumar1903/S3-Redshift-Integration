import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stagingevents;"
staging_songs_table_drop = "DROP TABLE IF EXISTS stagingsongs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS stagingevents (artist character varying(max),auth character varying(50),
firstName character varying(50), gender char(1), itemInSession integer, lastName character varying(50), length real,
level character varying(10), location character varying(max), method char(10), page character varying(50), registration real, sessionId integer, song character varying(max), status integer, ts timestamp, userAgent character varying(max), userId integer);""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS stagingsongs (num_songs integer, artist_id character varying(max), artist_latitude numeric, artist_longitude numeric, artist_location character varying(max), artist_name character varying(max), song_id character varying(max), title character varying(max), duration numeric, year integer); """)

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id int identity(0,1) PRIMARY KEY NOT NULL, start_time timestamp NOT NULL, user_id integer NOT NULL, level varchar(50), song_id character varying(max) NOT NULL, artist_id character varying(max) NOT NULL, session_id integer, location character varying(max), user_agent character varying(max)) DISTKEY(songplay_id) SORTKEY(songplay_id);""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id integer PRIMARY KEY NOT NULL sortkey, first_name varchar(50),last_name varchar(50), gender char(1), level varchar(50));""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id character varying(500) PRIMARY KEY NOT NULL distkey, title character varying(max), artist_id character varying(max) NOT NULL, year integer, duration numeric);""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id character varying(max) PRIMARY KEY NOT NULL distkey, artist_name character varying(max), artist_location character varying(max), lattitude numeric, longitude numeric);""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time timestamp PRIMARY KEY NOT NULL distkey, hour integer, day integer, week integer, month integer, year integer, weekday integer);""")

# STAGING TABLES

staging_events_copy = ("""COPY stagingevents FROM {} \
                     iam_role '{}' \
                     FORMAT AS JSON {} timeformat 'epochmillisecs';""").format(config.get("S3","LOG_DATA"), config.get("IAM_ROLE","ARN"), 
                     config.get("S3","LOG_JSONPATH"))             

staging_songs_copy = ("""COPY stagingsongs FROM {}
                    iam_role '{}'
                    FORMAT AS JSON 'auto';""").format(config.get("S3","SONG_DATA"), config.get("IAM_ROLE","ARN"))

# FINAL TABLES

songplay_table_insert = ("""insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) select se.ts as start_time, se.userId, se.level, ss.song_id, ss.artist_id, se.sessionId, se.location, se.userAgent 
FROM stagingevents se
 JOIN stagingsongs ss
 ON se.song = ss.title AND se.artist = ss.artist_name
 where se.userId is NOT NULL
 and
 ss.song_id is NOT NULL
 and
 ss.artist_id is NOT NULL
""")

user_table_insert = ("""Insert into users (user_id, first_name, last_name, gender, level) select distinct userId, firstName, lastName, gender, level from stagingevents where userId is NOT NULL""")

song_table_insert = ("""Insert into songs (song_id, title, artist_id, year, duration) select distinct song_id, title, artist_id, year, duration from stagingsongs where song_id is NOT NULL""")

artist_table_insert = ("""Insert into artists (artist_id, artist_name, artist_location, lattitude, longitude) select distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude from stagingsongs where artist_id is NOT NULL""")

time_table_insert = ("""Insert into time (start_time, hour, day, week, month, year, weekday) select distinct ts as start_time, extract(hour from ts) as hour, extract(day from ts) as day, extract(week from ts) as week, extract(month from ts) as month, extract( year from ts) as year, extract(weekday from ts) weekday from stagingevents""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]


##count_tables_queries = [stagingevents_count, stagingsongs_count, songplays_count, users_count, artists_count, time_count, songs_count]

# COUNT TABLES

##stagingevents_count = (""" SELECT COUNT(*) FROM stagingevents """)
##stagingsongs_count = (""" SELECT COUNT(*) FROM stagingsongs """)
##songplays_count = (""" SELECT COUNT(*) FROM songplays """)
##users_count = (""" SELECT COUNT(*) FROM users """)
##artists_count = (""" SELECT COUNT(*) FROM artists """)
##time_count = (""" SELECT COUNT(*) FROM time """)
##songs_count = (""" SELECT COUNT(*) FROM songs """)