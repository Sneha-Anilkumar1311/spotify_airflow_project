import json
import time
from kafka import KafkaProducer
import spotipy
from spotipy.oauth2 import SpotifyOAuth

# Spotify authentication
sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    client_id="d9a23f08f04e4b628bbb67a1b79c4766",
    client_secret="e4723b549ec044d3a78116818fe3bb65",
    redirect_uri="http://127.0.0.1:8000/callback",
    scope="user-read-recently-played"
))

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def get_recent_tracks():
    results = sp.current_user_recently_played(limit=5)

    for item in results.get('items', []):
        track = item['track']
        data = {
            'track_name': track['name'],
            'artist': track['artists'][0]['name'],
            'album': track['album']['name'],
            'played_at': item['played_at']
        }

        print("Sending:", data)
        producer.send('spotify_topic', value=data)
        producer.flush()
        time.sleep(1)

if __name__ == "__main__":
    get_recent_tracks()

