import spotipy
from spotipy.oauth2 import SpotifyOAuth
import os
from dateutil import parser
import pandas as pd
import uuid
import sqlalchemy as sq
from datetime import datetime as dt
import sys








sp = spotipy.Spotify(auth_manager=SpotifyOAuth(os.environ["SPOTIPY_CLIENT_ID"],
                                                   client_secret=os.environ["SPOTIPY_CLIENT_SECRET"],
                                                   redirect_uri=os.environ["SPOTIPY_REDIRECT_URI"],
                                                   scope="user-read-recently-played"))
recently_played = sp.current_user_recently_played(limit=50)

if len(recently_played) == 0:
    sys.exit("No results recieved from Spotify")

#album table
album_data = []
for row in recently_played['items']:
     album_id = row['track']['album']['id']
     album_name = row['track']['album']['name']
     album_release = row['track']['album']['release_date']
     album_url = row['track']['album']['external_urls']['spotify']
     album_total = row['track']['album']['total_tracks']
     artist_id = row['track']['album']['artists'][0]['id']
     album_uri = row['track']['album']['uri']
     dict2 = {"album_id" : album_id, "album_uri" : album_uri, "album_name" : album_name,
              "album_release" : album_release, "album_url" : album_url,
              "album_total" : album_total, "artist_id" : artist_id}
     album_data.append(dict2)

#artist table
artist_data = []
for row in recently_played['items']:
     artist_id = row['track']['artists'][0]['id']
     artist_name = row['track']['artists'][0]['name']
     artist_url = row['track']['artists'][0]['external_urls']['spotify']
     artist_uri = row['track']['artists'][0]['uri']
     artist_dict = {"artist_id" : artist_id, "artist_name" : artist_name,
                    "artist_uri": artist_uri,"artist_url" : artist_url}
     artist_data.append(artist_dict)
#song_table
song_data = []
for row in recently_played['items']:
     track_id = row['track']['id']
     album_id = row['track']['album']['id']
     artist_id = row['track']['artists'][0]['id']
     played_at = parser.parse(row['played_at'])
     track_url = row['track']['external_urls']['spotify']
     track_uri = row['track']['uri']
     duration = row['track']['duration_ms']
     song_dict = {"track_id" : track_id, "album_id" : album_id, "artist_id" : artist_id, "track_uri" : track_uri,
                  "played_at": played_at, "track_url": track_url, "duration": duration}
     song_data.append(song_dict)

album_df = pd.DataFrame.from_dict(album_data)
song_df = pd.DataFrame.from_dict(song_data)
artist_df = pd.DataFrame.from_dict(artist_data)


#drop duplicates from both tables artists and album.
artist_df = artist_df.drop_duplicates(subset=['artist_id'])
album_df = album_df.drop_duplicates(subset=['album_id'])
# add uniqueidentifiers to every single DF.
album_df['album_UUID'] = [str(uuid.uuid4()) for x in range(len(album_df))]
artist_df['artist_UUID'] = [str(uuid.uuid4()) for x in range(len(artist_df))]
song_df['song_UUID'] = [str(uuid.uuid4()) for x in range(len(song_df))]

#Clear up data to be only from today.
song_df = song_df[song_df['track_id'].isin(song_df[pd.to_datetime(song_df['played_at']).dt.date == dt.now().date()]['track_id'])]
if song_df.empty:
    sys.exit("No results recieved from Spotify from Today.")
album_df = album_df[album_df['album_id'].isin(song_df['album_id'])]
artist_df = artist_df[artist_df['artist_id'].isin(song_df['artist_id'])]

#convert to datetime or date
song_df['played_at'] = pd.to_datetime(song_df['played_at'])
album_df['album_release'] = pd.to_datetime(album_df['album_release'])
'''
artist_id = row['track']['artists'][0]['id']
artist_name = row['track']['artists'][0]['name']
artist_url = row['track']['artists'][0]['external_urls']['spotify']
artist_uri = row['track']['artists'][0]['uri']

'''
#function to create table.
metadata = sq.MetaData()
Artists_Table = sq.Table(
    "artists",
    metadata,
    sq.Column("artist_id", sq.Text, primary_key=True),
    sq.Column("artist_name", sq.Text),
    sq.Column("artist_url", sq.Text),
    sq.Column("artist_uri", sq.Integer),
    sq.Column("artist_UUID", sq.Text)
)
Album_Table = sq.Table(
    "albums",
    metadata,
    sq.Column("album_id", sq.Text, primary_key=True),
    sq.Column("album_name", sq.Text, nullable=False),
    sq.Column("album_release", sq.DateTime),
    sq.Column("album_total", sq.Integer),
    sq.Column("album_url", sq.Text),
    sq.Column("artist_id", sq.Text, sq.ForeignKey("artists.artist_id"), nullable=False),
    sq.Column("album_uri", sq.Text),
    sq.Column("album_UUID", sq.Text)
)
Songs_Table = sq.Table(
    "songs",
    metadata,
    sq.Column("track_id", sq.Text, primary_key=True),
    sq.Column("album_id", sq.Text, sq.ForeignKey("albums.album_id")),
    sq.Column("artist_id", sq.Text, sq.ForeignKey("artists.artist_id")),
    sq.Column("played_at", sq.DATETIME),
    sq.Column("track_url", sq.Text),
    sq.Column("track_uri", sq.Text),
    sq.Column("duration", sq.Text),
    sq.Column("song_UUID", sq.Text)
)
engine = sq.create_engine("sqlite:///spotify_etl2.db", echo=True, future=True)
metadata.create_all(engine, checkfirst=True)

with engine.connect() as conn:
    table_list = []
    for key in metadata.tables.keys():
        sql_query = f"CREATE TABLE IF NOT EXISTS {key}_TEMP AS SELECT * FROM {key} LIMIT 0"
        table_list.append(f'{key}_TEMP')
        data = conn.exec_driver_sql(sql_query)

    album_df.to_sql("albums_TEMP", con=engine, if_exists='append', index=False,chunksize=500)
    artist_df.to_sql("artists_TEMP", con=engine, if_exists='append', index=False,chunksize=500)
    song_df.to_sql("songs_TEMP", con=engine, if_exists='append', index=False,chunksize=500)
    my_dict = {'artists':'artist_id', 'albums':'album_id', 'songs':'track_id'}
    for key in my_dict:
        sql_query = f"INSERT OR REPLACE INTO {key} SELECT {key}_temp.* from {key}_TEMP LEFT JOIN {key} on {key}.{my_dict[key]} = {key}_temp.{my_dict[key]} where {key}.{my_dict[key]} IS NULL"
        data = conn.exec_driver_sql(sql_query)
    for key in table_list:
        drop_query = f"DROP TABLE [{key}];"
        data = conn.exec_driver_sql(drop_query)