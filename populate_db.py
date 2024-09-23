import glob
import sqlite3
import uuid
import pandas as pd

DB_FILENAME = 'data.db'

def get_spotify_history():
    history_files = sorted(glob.glob('spotify/StreamingHistory_music_*.json'))
    history_dfs = [pd.read_json(file) for file in history_files]
    spotify_df = pd.concat(history_dfs, ignore_index=True)
    spotify_df['endTime'] = pd.to_datetime(spotify_df['endTime'])
    spotify_df['startTime'] = spotify_df['endTime'] - pd.to_timedelta(spotify_df['msPlayed'] / 1000, unit='s')
    spotify_df['uuid'] = spotify_df.apply(lambda _: uuid.uuid4(), axis=1)
    spotify_df['uuid'] = spotify_df['uuid'].astype(str)
    return spotify_df

def get_uber_history():
    trips_df = pd.read_json('uber/trips.json')
    # Change to using the same terminology as in the Spotify data, for readability
    trips_df.rename(columns={'timestamp': 'startTime'}, inplace=True)
    trips_df['endTime'] = trips_df['startTime'] + pd.to_timedelta(trips_df['duration'], unit='s')
    return trips_df

def load_spotify():
    conn = sqlite3.connect(DB_FILENAME)
    df = get_spotify_history()
    df.to_sql('spotify_history', conn, if_exists='replace', index=False)
    conn.close()


def load_uber():
    conn = sqlite3.connect(DB_FILENAME)
    df = get_uber_history()
    dtype = {
        'pickup_zipcode': 'TEXT',
        'dropoff_zipcode': 'TEXT',
        'startTime': 'TIMESTAMP',
        'endTime': 'TIMESTAMP',
        'uuid': 'TEXT',
        'date': 'TIMESTAMP',
        'time': 'TEXT',
        'day': 'TEXT',
        'day_of_week': 'INTEGER',
        'sortable_day_of_week': 'TEXT',
        'season': 'TEXT',
        'type': 'TEXT',
        'earnings': 'REAL',
        'tip': 'REAL',
        'surge': 'REAL',
        'duration': 'INTEGER',
        'distance': 'REAL',
        'pickup_address': 'TEXT',
        'dropoff_address': 'TEXT',
        'earnings-surge': 'REAL',
        'earnings/second': 'REAL',
        'earnings/mile': 'REAL'
    }
    df.to_sql('uber_trips', conn, if_exists='replace', dtype=dtype, index=False)
    conn.close()

if __name__=="__main__":
    load_spotify()
    load_uber()
    print('Done!')