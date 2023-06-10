


def main():


    import requests
    import pandas as pd
    import datetime
    import time
    import spotipy
    from fuzzywuzzy import fuzz
    from spotipy.oauth2 import SpotifyClientCredentials
    from google.cloud import storage
    from google.cloud.storage.bucket import Bucket
    from google.cloud import secretmanager

    def search_spotify_tracks(artist_name):

        while True:
            try:
                results = spotify.search(q=artist_name, type='artist', limit=5)

                break  # Exit the loop if the request is successful
            except spotipy.client.SpotifyException as e:
                if e.http_status == 429:
                    print("Rate limit exceeded. Waiting and retrying...")
                    time.sleep(60)  # Wait before retrying
                else:
                    print("Error occurred while making the request:", e)
                    return []

        if results and results['artists']['items']:

            fuzz_list = [fuzz.partial_ratio(results['artists']['items'][x]['name'], artist_name) for x in range(len(results['artists']['items']))]

            max_artist_idx = fuzz_list.index(max(fuzz_list))

            if fuzz.partial_ratio(results['artists']['items'][max_artist_idx]['name'], artist_name)  > 80:

                artist = results['artists']['items'][max_artist_idx]
                artist_id = artist['id']
                artist_info = {
                    'seatgeek_artist_name':artist_name,
                    'spotify_artist_name': artist['name'],
                    'spotify_artist_id': artist['id'],
                    'spotify_artist_uri': artist['uri'],
                    'spotify_artist_image': artist['images'][0]['url'] if artist['images'] else None,
                    'spotify_genres': artist['genres']
                }
            else:
                artist_info = {
                    'spotify_artist_name': None,
                    'spotify_artist_id': None,
                    'spotify_artist_uri': None,
                    'spotify_artist_image': None,
                    'spotify_genres': None
                }


            top_tracks = spotify.artist_top_tracks(artist_id)
            tracks_data = []
            for track in top_tracks['tracks']:

                audio_features = get_track_audio_features(track['id'])

                track_info = {
                    'track_name': track['name'],
                    'track_uri': track['uri'],
                    'track_image': track['album']['images'][0]['url'] if track['album']['images'] else None,
                    'popularity': track['popularity'],
                    'danceability': audio_features['danceability'],
                    'energy': audio_features['energy'],
                    'key':audio_features['key'],
                    'loudness': audio_features['loudness'],
                    'mode': audio_features['mode'],
                    'speechiness': audio_features['speechiness'],
                    'acousticness': audio_features['acousticness'],
                    'instrumentalness': audio_features['instrumentalness'],
                    'liveness':audio_features['liveness'],
                    'valence': audio_features['valence'],
                    'tempo': audio_features['tempo'],
                    'track_id': audio_features['id'],
                    'track_href': audio_features['track_href'],
                    'duration_ms': audio_features['duration_ms'],
                    'time_signature': audio_features['time_signature']}

                track_info.update(artist_info)
                tracks_data.append(track_info)
            return tracks_data
        else:
            print(f"No artist found for name: {artist_name}")
        return []



    def get_track_audio_features(track_id):
        audio_features = spotify.audio_features(track_id)
        if audio_features:
            return audio_features[0]
        else:
            return {}



    def get_artist_genres(artist_name):
        results = spotify.search(q=artist_name, type='artist', limit=1)
        if results and results['artists']['items']:
            artist = results['artists']['items'][0]
            return artist['genres']
        else:
            print(f"No artist found for name: {artist_name}")
            return []



    def get_secret_from_secret_manager(project_id, secret_name):
        client = secretmanager.SecretManagerServiceClient()

        # Build the secret name
        secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"

        # Access the secret payload
        response = client.access_secret_version(secret_path)
        secret_value = response.payload.data.decode("UTF-8")

        return secret_value



    # Retrieve the secret keys from Secret Manager

    # Set up SeatGeek API parameters
    seatgeek_client_id =  get_secret_from_secret_manager('upcoming-local-shows', 'seatgeek-client-id') 
    seatgeek_secret = get_secret_from_secret_manager('upcoming-local-shows', 'seatgeek-secret')        
    seatgeek_base_url = 'https://api.seatgeek.com/2/events'

    # Set up Spotify API parameters
    spotify_client_id = get_secret_from_secret_manager('upcoming-local-shows', 'spotify-client-id')    
    spotify_client_secret = get_secret_from_secret_manager('upcoming-local-shows', 'spotify-secret')   
    spotify_client_credentials_manager = SpotifyClientCredentials(client_id=spotify_client_id,
                                                                  client_secret=spotify_client_secret)
    spotify = spotipy.Spotify(client_credentials_manager=spotify_client_credentials_manager)



    # Initialize the GCS client
    bucket_name = 'upcoming-local-shows'

    client = storage.Client()
    bucket = Bucket.from_string(f"gs://{bucket_name}", client=client)

    # Get the blob object for the CSV file
    existing_data_path = 'concert_data.csv'
    blob = bucket.blob(existing_data_path)

    # Download the CSV file to a temporary file
    temp_file = '/tmp/temp.csv'  # You can choose any temporary file path
    blob.download_to_filename(temp_file)

    # Read the CSV file into a pandas DataFrame
    existing_data = pd.read_csv(temp_file)


    # Set up today's date
    today = datetime.date.today()

    # Set up search parameters for SeatGeek API
    states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", 
                  "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", 
                  "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]  
    taxonomy = 'concert'
    start_date = today.strftime('%Y-%m-%d')

    # Perform search on SeatGeek API
    events_data = []
    for state in states:
        params = {
            'client_id': seatgeek_client_id,
            'client_secret': seatgeek_secret,
            'datetime_utc.gte': start_date,
            'taxonomies.name': taxonomy,
            'venue.state': state,
        }
        response = requests.get(seatgeek_base_url, params=params)
        data = response.json()
        events_data.extend(data['events'])

    # Create a DataFrame from the SeatGeek API response
    events_df = pd.json_normalize(events_data)
    events_df.rename(columns={'performers': 'artists'}, inplace=True)

    # Expand 'artists' column and rename to 'artist_name'
    artists_df = events_df.explode('artists').reset_index(drop=True)
    artists_df['artist_name'] = artists_df['artists'].apply(lambda x: x.get('name'))
    artists_df['type'] = artists_df['artists'].apply(lambda x: x.get('type'))

    # Extract genres as a list and append as a column
    artists_df['genres'] = artists_df['artist_name'].apply(lambda artist: get_artist_genres(artist))

    # Cross reference with existing DataFrame to get new records
    new_records = artists_df[~artists_df['id'].astype(str).isin(existing_data['id'].astype(str))].reset_index(drop=True)
    old_records = existing_data[existing_data['id'].astype(str).isin(artists_df['id'].astype(str))].reset_index(drop=True)

    # Convert datetime to date
    new_records['datetime_utc'] = new_records['datetime_utc'].apply(lambda x: pd.to_datetime(x).date())


    new_records = new_records[['id', 'type',
                                'datetime_utc', 'artist_name', 'genres','datetime_local',  'short_title','url',
                                'title', 'venue.state','venue.name_v2', 'venue.postal_code', 'venue.name',
                                'venue.url', 'venue.location.lat','venue.location.lon', 'venue.address', 
                                'venue.country','venue.city','venue.extended_address','venue.capacity',
                                'venue.display_location', 'stats.listing_count', 'stats.average_price',
                                'stats.lowest_price','stats.highest_price']]



    # Search Spotify API for track and artist information
    tracks_data = []
    already_had_tracks_df = pd.DataFrame()
    for _, row in new_records.iterrows():

        artist_name = row['artist_name']

        if artist_name in existing_data['artist_name'].unique().tolist():

            temp_df = existing_data[existing_data['seatgeek_artist_name'] == artist_name][['track_name', 'track_uri', 'track_image',
                                                                                'popularity', 'danceability', 'energy', 'key', 'loudness', 'mode',
                                                                                'speechiness', 'acousticness', 'instrumentalness', 'liveness',
                                                                                'valence', 'tempo', 'track_id', 'track_href', 'duration_ms',
                                                                                'time_signature', 'seatgeek_artist_name', 'spotify_artist_name',
                                                                                'spotify_artist_id', 'spotify_artist_uri', 'spotify_artist_image',
                                                                                'spotify_genres']]

            already_had_tracks_df = pd.merge(pd.DataFrame(row).T, temp_df, how = 'cross')

        else:

            try:
                tracks = search_spotify_tracks(artist_name)
                tracks_data.extend(tracks)
            except:
                print(f"No fuzzy match found for artist: {artist_name}")

    # Create a DataFrame from the Spotify API response
    tracks_df = pd.DataFrame(tracks_data)
    # Join to SeatGeek API data
    final_df = pd.merge(new_records, tracks_df, how = 'inner', left_on ='artist_name', right_on = 'seatgeek_artist_name' )
    # Join back in existing artists
    final_df = pd.concat([final_df, already_had_tracks_df])
    # Join back in old records
    final_df = pd.concat([final_df, old_records])

    # Print New Shows added
    print(f"Added {len(new_records)} new shows")

    # Drop duplicates
    final_df = final_df.drop_duplicates(subset= ['id', 'track_name', 'datetime_utc', 'artist_name'])

    # Only future dates
    final_df = final_df[final_df['datetime_utc'].astype(str) >= start_date]

    # Add the load date
    final_df['LOAD_DATE'] = today

    # Print Total Shows 
    print(f"There are {len(final_df)} shows total")

    # Convert the DataFrame to CSV format as a string
    new_data_path = 'concert_data.csv'
    csv_data = final_df.to_csv(index=False)

    # Create a client to interact with Google Cloud Storage
    client = storage.Client()

    # Get the bucket to which the CSV file will be uploaded
    bucket = client.bucket(bucket_name)

    # Create the blob (file) object for the CSV file
    blob = bucket.blob(new_data_path)

    # Upload the CSV data to the blob
    blob.upload_from_string(csv_data, content_type='text/csv')
    print(f"Data Frame saved as {new_data_path} in {bucket_name}")
    print('CSV file uploaded successfully.')




if __name__ == '__main__':
    main()
