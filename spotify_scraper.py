import json

import requests
import pandas as pd
import time

from pandas import read_csv

from keys import SPOTIFY_KEYS

# Spotify API credentials (you should probably use env variables but i am lazy)
CLIENT_ID = SPOTIFY_KEYS.CLIENT_ID
CLIENT_SECRET = SPOTIFY_KEYS.CLIENT_SECRET

def get_access_token(client_id, client_secret):
    """Obtain an access token for Spotify API."""
    url = "https://accounts.spotify.com/api/token"
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {'grant_type': 'client_credentials'}
    response = requests.post(url, headers=headers, data=data, auth=(client_id, client_secret))
    response.raise_for_status()
    return response.json().get('access_token')


def get_artist_id_from_album(access_token, album_name, artist_name=None, retry_count=3):
    """Search for an album using the full album name and the first word of the artist's name, and fetch the artist's popularity."""
    url = "https://api.spotify.com/v1/search"
    headers = {'Authorization': f'Bearer {access_token}'}

    artist_first_word = artist_name.split()[0] if artist_name else None

    query = f'album:{album_name}'
    if artist_first_word:
        query += f' artist:{artist_first_word}'

    params = {
        'q': query,
        'type': 'album',
        'limit': 1
    }

    for attempt in range(retry_count):
        try:
            response = requests.get(url, headers=headers, params=params)

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 1))
                print(f"Rate limited. Retrying after {retry_after} seconds...")
                time.sleep(retry_after)
                continue

            response.raise_for_status()
            album_data = response.json().get('albums', {}).get('items', [])

            if album_data:
                album = album_data[0]
                first_artist = album['artists'][0]
                return first_artist.get('id', None)

            return None

        except requests.exceptions.RequestException as e:
            print(f"Error fetching album data: {e}. Attempt {attempt + 1}/{retry_count}")
            time.sleep(2 ** attempt)

    return None


def get_artist_popularity(access_token, artist_id, retry_count=3):
    """Fetch the popularity score of an artist using the artist's ID."""
    url = f"https://api.spotify.com/v1/artists/{artist_id}"
    headers = {'Authorization': f'Bearer {access_token}'}

    for attempt in range(retry_count):
        try:
            response = requests.get(url, headers=headers)

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 1))
                print(f"    Rate limited. Retrying after {retry_after} seconds...")
                time.sleep(retry_after)
                continue

            response.raise_for_status()
            artist_data = response.json()

            # Get artist popularity
            artist_popularity = artist_data.get('popularity', None)
            return artist_popularity

        except requests.exceptions.RequestException as e:
            print(f"  Error fetching artist popularity data: {e}. Attempt {attempt + 1}/{retry_count}")
            time.sleep(2 ** attempt)

    return None


def get_artist_popularity_scores(in_df):
    """Add a 'popularity_score' column to the dataframe."""
    out_df = in_df.copy()

    access_token = get_access_token(CLIENT_ID, CLIENT_SECRET)

    popularity_scores = []

    for index, row in df.iterrows():
        artist_name = row['artist']
        album_name = row['title']
        print(f"{index:4}: ", end="")
        try:
            artist_id = get_artist_id_from_album(access_token, album_name, artist_name)
            if artist_id:
                popularity = get_artist_popularity(access_token, artist_id)
                print(f"Successfully fetched popularity for [{album_name}] ({artist_name}): {popularity}")
                popularity_scores.append(popularity)
            else:
                print(f"  Error fetching artist id for [{album_name}] ({artist_name})")
                popularity_scores.append(None)

        except Exception as e:
            print(f"  Error fetching popularity for [{album_name}] ({artist_name}): {e}")
            popularity_scores.append(None)  # Append None if there's an error

    out_df['artist_popularity'] = popularity_scores
    return out_df


import kagglehub
path = kagglehub.dataset_download("tabibyte/aoty-5000-highest-user-rated-albums")
df = pd.read_csv(path + '/aoty.csv')

df_with_popularity = get_artist_popularity_scores(df)

print(df_with_popularity[['artist', 'artist_popularity']].head())

df_with_popularity.to_csv("aoty_with_popularity.csv", index=False)
