import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """Process json file, insert into artist and song table
    
    Key Arguments:
    cur : reference to connected db
    filepath: json filepath
    
    Output:
    Inserting data into db tables: artist table and song table
    """
    # open song file
    df =pd.DataFrame(pd.read_json(filepath,typ='series')).T

    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']].values.tolist()[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values.tolist()[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """Process json file, insert data into time table, user table and songplay table
    
    Key Arguments:
    cur : reference to connected db
    filepath: json filepath
    
    Output:
    Inserting data into db tables: time table, user table and songplay table
    """
    # open log file
    df = pd.DataFrame(pd.read_json(filepath,lines=True))

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    t = df.copy()
    t['ts']=pd.to_datetime(t['ts'],unit='ms')
    
    # insert time data records
    time_data =  [t['ts'].dt.strftime('%Y-%m-%d %I:%M:%S'),t['ts'].dt.hour,t['ts'].dt.day,t['ts'].dt.week,t['ts'].dt.month,t['ts'].dt.year,t['ts'].dt.weekday]

    column_labels = ['starttime','hour','day','week','month','year','weekday']
    zip_it=zip(column_labels,time_data)
    time_df = pd.DataFrame(dict(zip_it))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]
    
    #remove rows with missing user id
    user_df_clean = user_data[user_data['userId']!= '']
    
    #remove duplicates
    user_df_deduped = user_df_clean.drop_duplicates('userId',keep='first')    
    
    # insert user records
    for i, row in user_df_deduped.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        starttime=pd.to_datetime(row.ts,unit='ms').strftime('%Y-%m-%d %I:%M:%S')
        songplay_data = (starttime,row.userId,row.level,str(songid),str(artistid),row.sessionId,row.location,row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """Iterate through source data and apply the functions (process_log_file,process_song_file) on file level
    
    Key Arguments:
    cur : reference to connected db
    conn : connection to database
    filepath: json filepath
    func: appropriate function for source file (process_log_file or process_song_file)
    
    Output:
    Populating database tables using the source data
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
    """
    Create connection to the database and initiate the data processing procedure in order to populate the database tables
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb_mb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()