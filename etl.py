import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from sql_connection import *

def process_song_file(cur, filepath):
    """
    Description: This function is responsible for processing, organizing and
    committing ot the database the contents of a single file from the directory
    './data/song_data'

    Arguments:
        cur: the cursor object for query execution.
        filepath: single data song file path.
        
    Returns:
        None
    """
    print('Starting the process on ' + filepath)
    # open song file
    df = pd.read_json(filepath, typ='series');
    
    # insert song record
    # song_id, title, artist_id, year, duration
    song_data = (df.at['song_id'], df.at['title'], df.at['artist_id'], df.at['year'], df.at['duration']);
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    # artist_id, name, location, latitude, longitude;
    artist_data = (df.at['artist_id'], df.at['artist_name'], df.at['artist_location'], df.at['artist_latitude'], df.at['artist_longitude']);
    cur.execute(artist_table_insert, artist_data);


def process_log_file(cur, filepath):
    """
    Description: This function is responsible for processing, organizing and
    committing ot the database the contents of a single file from the directory
    './data/log_data'

    Arguments:
        cur: the cursor object for query execution.
        filepath: single data log file path.
        
    Returns:
        None
    """
    print('Starting the process on ' + filepath)
    # open log file
    file = open(filepath, 'r');
    fileString = file.read();
    lines = fileString.split('\n');
    linesSize = len(lines);
    s = pd.read_json(lines[0],typ='series');
    df = pd.DataFrame(s).transpose();
    for i in range(1,len(lines)):
        s = pd.read_json(lines[i],typ='series');
        df = df.append(pd.DataFrame(s).transpose(),ignore_index=True);

    # filter by NextSong action
    df = df.loc[df['page']=='NextSong']

    # convert timestamp column to datetime
    t = df.filter(items=['ts']).applymap(lambda x: pd.Timestamp(x, unit='ms'));
    
    # insert time data records
    # 'start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday'
    time_df = t.copy(deep=True).rename(columns = {'ts': 'start_time'})
    temp = t.applymap(lambda x: int(x.hour)).rename(columns = {'ts': 'hour'})
    time_df = time_df.join(temp, lsuffix='_caller', rsuffix='_other')
    temp = t.applymap(lambda x: int(x.day)).rename(columns = {'ts': 'day'})
    time_df = time_df.join(temp, lsuffix='_caller', rsuffix='_other')
    temp = t.applymap(lambda x: int(x.week)).rename(columns = {'ts': 'week'})
    time_df = time_df.join(temp, lsuffix='_caller', rsuffix='_other')
    temp = t.applymap(lambda x: int(x.month)).rename(columns = {'ts': 'month'})
    time_df = time_df.join(temp, lsuffix='_caller', rsuffix='_other')
    temp = t.applymap(lambda x: int(x.year)).rename(columns = {'ts': 'year'})
    time_df = time_df.join(temp, lsuffix='_caller', rsuffix='_other')
    temp = t.applymap(lambda x: int(x.dayofweek)).rename(columns = {'ts': 'dayofweek'})
    time_df = time_df.join(temp, lsuffix='_caller', rsuffix='_other')

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    # user_id, first_name, last_name, gender, level
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    # songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    for index, row in df.iterrows():
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (time_df['start_time'][index], row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Description: This function is responsible for listing the files in a directory,
    and then executing the ingest process for each file according to the function
    that performs the transformation to save it to the database.

    Arguments:
        cur: the cursor object.
        conn: connection to the database.
        filepath: log data or song data file path.
        func: function that transforms the data and inserts it into the database.

    Returns:
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
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect(host = dbhost, dbname = dbnameProject, user = userProject, password = passwordProject)
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()