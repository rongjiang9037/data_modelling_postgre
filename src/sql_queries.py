# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS SONGPLAYS (songplayid SERIAL PRIMARY KEY, 
                                      start_time TIMESTAMP NOT NULL, 
                                      user_id INT NOT NULL, 
                                      level VARCHAR, 
                                      song_id VARCHAR, 
                                      artist_id VARCHAR, 
                                      session_id INT, 
                                      location VARCHAR,
                                      user_agent VARCHAR,
                            UNIQUE (start_time, user_id, song_id));
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS USERS (user_id INT PRIMARY KEY, 
                                  first_name VARCHAR,
                                  last_name VARCHAR, 
                                  gender VARCHAR,
                                  level VARCHAR);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS SONGS (song_id VARCHAR PRIMARY KEY, 
                                  title VARCHAR,
                                  artist_id VARCHAR, 
                                  year INT, 
                                  duration NUMERIC);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS ARTISTS (artist_id VARCHAR PRIMARY KEY, 
                                    name VARCHAR,
                                    location VARCHAR, 
                                    latitude NUMERIC, 
                                    longitude NUMERIC)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS TIME (start_time TIMESTAMP PRIMARY KEY,
                                 hour INT,
                                 day INT,
                                 week INT,
                                 month INT,
                                 year INT,
                                 weekday INT)
""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays
                                (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (start_time, user_id, song_id)
                            DO NOTHING
""")

user_table_insert = ("""INSERT INTO USERS 
                            VALUES (%(user_id)s, 
                                    %(first_name)s, 
                                    %(last_name)s, 
                                    %(gender)s, 
                                    %(level)s)
                        ON CONFLICT (user_id)
                        DO UPDATE SET level = excluded.level
""")

song_table_insert = ("""INSERT INTO songs 
                            (song_id, title, artist_id, year, duration)
                            VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (song_id)
                        DO NOTHING
""")

artist_table_insert = ("""INSERT INTO ARTISTS 
                                (ARTIST_ID, NAME, LOCATION, LATITUDE, LONGITUDE)
                                VALUES (%s, %s, %s, %s, %s)
                          ON CONFLICT (artist_id)
                          DO NOTHING
""")


time_table_insert = ("""INSERT INTO time 
                            VALUES (%(start_time)s, 
                                    %(hour)s, 
                                    %(day)s, 
                                    %(week)s, 
                                    %(month)s, 
                                    %(year)s, 
                                    %(weekday)s)
                        ON CONFLICT (start_time)
                        DO NOTHING
""")

# FIND SONGS

song_select = ("""SELECT SONGS.SONG_ID, SONGS.ARTIST_ID
                    FROM SONGS JOIN ARTISTS
                    ON SONGS.ARTIST_ID = ARTISTS.ARTIST_ID
                    WHERE SONGS.TITLE=%s AND ARTISTS.NAME=%s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]