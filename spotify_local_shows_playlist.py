

################################################################################################################
import requests
import os
from dotenv import load_dotenv

import datetime

from bs4 import BeautifulSoup
import re
from fuzzywuzzy import fuzz

import spotipy
from spotipy.oauth2 import SpotifyOAuth
################################################################################################################

# get and format today's date
today = datetime.datetime.today().date().strftime('%Y-%m-%d')

# grab environment variables
load_dotenv()
SPOTIFY_CLIENT_ID = os.getenv('SPOTIFY_CLIENT_ID')
SPOTIFY_CLIENT_SECRET = os.getenv('SPOTIFY_CLIENT_SECRET')
REDIRECT_URI = os.getenv('REDIRECT_URI')

################################################################################################################
"""
Connects to website and scrapes a list of urls containing upcoming bands.

Inputs:     Today's date
Outputs:    A list of raw urls containing band names
"""


def scrape_bands(todays_date):

    # connect to website
    url = f'https://www.bandsintown.com/?date={todays_date}&date_filter=This+Week'
    headers = {'User-Agent': 'USERAGENT'}
    response = requests.get(url, headers=headers)

    # check for valid response status
    if response.status_code != 200:
        raise Exception("There has been an error in scraping the website, response error code:  ", response.status_code)

    # start the soup
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # make some soup
    link_list = []

    for link in soup.find_all('a', href=True, attrs={'href': re.compile(r"^https://www.bandsintown.com")}):
        # display the actual urls
        link_list.append(link.get('href'))

    raw_urls = [link for link in link_list if link.startswith("https://www.bandsintown.com/e/")]

    return  raw_urls

################################################################################################################
"""
Cleans up the raw data into searchable and unique band names.

Inputs:     A list of urls containing band names
Outputs:    Just the band names
"""


def process_bands(raw_urls):
    # grab the 'event's near' info from bottom of website
    pattern = re.compile(r"\b\S*-at\b")
    reg_list = [re.search(pattern , str(x)).group(0) for x in raw_urls]

    # select the link from the original grab
    reg_list = [x.replace('-at','').replace('-', ' ') for x in reg_list]

    # select the band name after the space
    # this should work even for bands that start with numbers!
    pattern = re.compile(r"\s(.*)")
    reg_list = [re.search(pattern, str(x)).group(0) for x in reg_list]

    # get rid of parentheses and commas (and nested lists by default)
    reg_list = [x.replace("'",'').replace(',', '') for x in reg_list]

    # drop duplicates by using a set
    bands = list(set(reg_list))

    return bands

################################################################################################################
"""
Authenticates with Spotify and spotipy, then creates a new playlist.

Inputs:     A list of urls containing band names
Outputs:    Current user id and current playlist id
"""


def authenticate(client_id, secret,redirect_uri):
    # authenticate
    sp = spotipy.Spotify(
        auth_manager=SpotifyOAuth(scope="playlist-modify-public",
                                redirect_uri=redirect_uri,
                                client_id=client_id,
                                client_secret=secret,
                                show_dialog=True,
                                cache_path="token.txt")
                        )
    # grab user id
    u_id = sp.current_user()["id"]

    return u_id, sp

################################################################################################################
"""
Search names of bands and find a few of their songs, adding those track ids to a list

Inputs:     A list of band names
Outputs:    A list containing 5 track ids per band
"""


# function to
def grab_track_ids(bands, sp):
    tracks = []
    for i in range(len(bands)):
        results = sp.search(q=f"{bands[i]}", limit=5, type='track')
        if results['tracks']['total'] == 0:
            continue
        else:
            for j in range(len(results['tracks']['items'])):
                if fuzz.partial_ratio(results['tracks']['items'][j]['artists'][0]['name'], bands[i]) > 80:
                    tracks.append(results['tracks']['items'][j]['id'])
                else:
                    continue
    return tracks

################################################################################################################

"""
Adds track ids to newly created playlist, or replaces tracks in playlist if already created

Inputs:     A list of track ids, user_id, and the playlist id
Outputs:    None
"""
def add_tracks_to_playlist(u_id, tracks, sp):

    list_of_playlists = sp.user_playlists(u_id, limit=50, offset=0)

    for p in list_of_playlists['items']:

        if p['name'] == 'Upcoming Local Shows Playlist':
            p_id = p['id']
            sp.user_playlist_replace_tracks(u_id, p_id, tracks)
            break
        else:
            # create a new playlist
            playlist = sp.user_playlist_create(user=u_id,
                                               name=f"Upcoming Local Shows Playlist",
                                               public=True,
                                               collaborative=False,
                                               description="Songs from artists that will be playing in your area this coming week")
            # grab playlist id
            p_id = playlist["id"]

            # add tracks to new playlist
            sp.user_playlist_add_tracks(u_id, p_id, tracks)
            break

    return

################################################################################################################

if __name__ == '__main__':


    try:
        event_urls = scrape_bands(today)
    except:
        print('Trouble scraping the website')

    try:
        band_list = process_bands(event_urls)
    except:
        print('Trouble cleaning up band names')

    try:
        user_id, sp = authenticate(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET, REDIRECT_URI)
    except:
        print('Trouble authenticating with Spotify')

    try:
        track_ids = grab_track_ids(band_list, sp)
    except:
        print('Trouble accessing tracks from Spotify')

    try:
        add_tracks_to_playlist(user_id, track_ids, sp)
    except:
        print('Trouble creating playlists on Spotify')
