# DROP TABLES

songplay_table_drop = "drop table if exists songplays;"
user_table_drop = "drop table if exists users;"
song_table_drop = "drop table if exists songs;"
artist_table_drop = "drop table if exists artists;"
time_table_drop = "drop table if exists time;"

# CREATE TABLES


user_table_create = ("""create table if not exists users (user_id int PRIMARY KEY NOT NULL UNIQUE, first_name varchar, last_name varchar,\
                        gender varchar, level varchar);
""")

song_table_create = ("""create table if not exists songs (song_id varchar PRIMARY KEY NOT NULL UNIQUE, title varchar, artist_id varchar, year int, duration float);
""")

artist_table_create = ("""create table if not exists artists (artist_id varchar PRIMARY KEY UNIQUE, name varchar, location varchar,\
                        latitude float, longitude float);
""")

time_table_create = ("""create table if not exists time (start_time date PRIMARY KEY UNIQUE, hour int, day int, week int, \
                        month int, year int, weekday int);
""")

songplay_table_create = ("""create table if not exists songplays (songplay_id int PRIMARY KEY NOT NULL UNIQUE, start_time date NOT NULL, \
                            user_id int NOT NULL, level varchar, song_id varchar, artist_id varchar, session_id int, \
                            location varchar, user_agent varchar, FOREIGN KEY (start_time) REFERENCES time(start_time), \
                            FOREIGN KEY (user_id) REFERENCES users(user_id), FOREIGN KEY (song_id) REFERENCES songs(song_id), \
                            FOREIGN KEY (artist_id) REFERENCES artists(artist_id));
""")

# INSERT RECORDS

songplay_table_insert = ("""insert into songplays (songplay_id, start_time, user_id, level, \
                            song_id, artist_id, session_id, location, user_agent) values \
                            (%s, %s, %s, %s, %s, %s, %s, %s, %s) on conflict do nothing;
""")

user_table_insert = ("""insert into users (user_id, first_name, last_name, gender, level) values \
                        (%s, %s, %s, %s, %s) on conflict (user_id) do update set level='paid';
""")

song_table_insert = ("""insert into songs (song_id, title, artist_id, year, duration) values \
                        (%s, %s, %s, %s, %s) on conflict do nothing;
""")

artist_table_insert = ("""insert into artists (artist_id, name, location, latitude, longitude) values \
                        (%s, %s, %s, %s, %s) on conflict do nothing;
""")


time_table_insert = ("""insert into time (start_time, hour, day, week, month, year, weekday) values \
                        (%s, %s, %s, %s, %s, %s, %s) on conflict do nothing;
""")

# FIND SONGS

song_select = ("""select distinct song_id, a.artist_id from songs a \
                join artists b on a.artist_id = b.artist_id \
                where a.title='%s' and b.name='%s' and a.duration=%s;
                """)

# QUERY LISTS

create_table_queries = [ user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]