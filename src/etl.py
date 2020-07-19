import os
import glob
import time
import psycopg2
import psycopg2.extras

from tqdm import tqdm
from memory_profiler import memory_usage

import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    This function extract songs & artist information,
    and store them into songs/artist tables.
    
    input:
    cur: cursor variable
    filepath: file path to the song file
    
    return: None
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    This function extract time & user & songplay information,
    and store them into teim/user/songplay tables.
    
    input:
    cur: cursor variable
    filepath: file path to the log file
    
    return: None
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.concat(time_data, axis=1)
    time_df.columns = column_labels

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, row.to_dict())

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    # rename columsn to match SQL query
    user_df = user_df.rename(columns={'userId':'user_id', 'firstName':'first_name', \
                                      'lastName':'last_name'})

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row.to_dict())

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)

def process_log_data_batch(cur, filepath):
    """
    This function extract time & user & songplay information,
    and store them into teim/user/songplay tables in a batch.
    
    input:
    cur: cursor variable
    filepath: file path to the log file
    
    return: None
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records in a batch
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.concat(time_data, axis=1)
    time_df.columns = column_labels
    
    psycopg2.extras.execute_batch(cur, time_table_insert, time_df.to_dict(orient='records'))

    # load user table in a batch
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    user_df = user_df.rename(columns={'userId':'user_id', 'firstName':'first_name', \
                                      'lastName':'last_name'})

    # insert user records
    psycopg2.extras.execute_batch(cur, user_table_insert, user_df.to_dict(orient='records'))

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)
        

def process_data(cur, conn, filepath, func):
    """
    This function extract all file pathes in "filepath" directory
    and process each file with the provided function.
    
    input:
    cur: cursor variable
    conn: database connection object
    filepath: file path that store all files
    func: data process function
    
    return:
    None
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
#     print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for datafile in tqdm(all_files): 
#     for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
#         print('{}/{} files processed.'.format(i, num_files))

def time_profile(func):
    """
    This function measure time & memory usage for function 'func'.
    
    input: 
    func: function to be measured
    return: None
    """
    def inner(*args, **kwargs):
        # print function name
        args_str = ', '.join([str(x.__name__) for x in args])
        kwargs_str = ', '.join([f'{k}={v}' for k, v in kwargs.items()])
        print(f'\nfunction: {func.__name__}({args_str} {kwargs_str})')
        
        # measure time
        start_time = time.time()
        func(*args, **kwargs)
        time_used = time.time() - start_time
        print(f'Time {time_used:0.4}')
        
    return inner
        
        
@time_profile
def main(process_song_file, process_log_file):
    """
    - Connect to sparkifydb database
    
    - get cursor variable
    
    - process data stored in file path 'data/song_data' & 'data/log_data'
    
    - close database connection
    
    input: None
    return: None
    """
    # connect to database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    # process data
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    # close database connection
    conn.close()


        

if __name__ == "__main__":
    main(process_song_file, process_log_file)
    main(process_song_file, process_log_data_batch)