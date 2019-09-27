import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """ Takes as input DMBS cursor and json datafile. The function reads the json file and finds pre-specified columns to
        add data to insert statements loaded from sql_queries. This is done for both songs and artists tables.
    """
    
    # open song file 
    df = pd.read_json(filepath, lines=True)
    
    # insert song record 
    song_data = [df.values[0][7], df.values[0][8], df.values[0][0], df.values[0][9], df.values[0][5]]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = [df.values[0][0], df.values[0][4], df.values[0][2], df.values[0][1], df.values[0][3]]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """ Takes as input DMBS cursor and json datafile. The function reads the json file,  filters not-nextsong actions and
        finds all data needed for insert statements for time, users and songplays tables. For songplays it utilizes select to 
        try to find the song_id and artist_id from the songs and artists tables as these are not present in the json file.
    """
    
    # open log file
    df = pd.read_json(filepath,lines=True)
    
    # filter by NextSong action
    df = df[df.page=="NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df["ts"], unit = "ms")
    
    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ("timestamp", "hour", "day", "week", "month", "year", "weekday")
    time_df = pd.DataFrame({column_labels[i]:time_data[i] for i in range(7)})

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = pd.DataFrame({"userId":df.userId, "firstName":df.firstName, "lastName":df.lastName, "gender":df.gender, "level":df.level})

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select %(row.song.replace("'",""), row.artist.replace("'", ""), row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (index, list(t[t.index==index])[0], row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """ Takes as input DBMS cursor and connection, filepath and function. It goes through all the files in the filepath, find ones with json extention
        and makes a list with path and file names. It then loops over all, passing the cursor and the json filepath to the input function.
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()