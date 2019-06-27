import csv

ARTIST_FIELDS = [
    'chartmetric_artist_id',
    'name',
    'avg_spotify_followers',
    'avg_fb_engagement',
    'avg_fb_likes',
    'avg_youtube_subscribers',
    'avg_youtube_engagement',
    'spotify_popularity',
    'spotify_followers',
    'spotify_monthly_listeners',
    'spotify_listeners_to_followers_ratio',
    'facebook_likes',
    'facebook_talks',
    'youtube_views',
    'youtube_subscribers',
    'wikipedia_views',
    'soundcloud_followers'
]

ARTIST_TRACK_FIELDS = [
    'artist_id',
    'track_id',
    'name',
    'release_dates'
]

rows = {
    'artists': ARTIST_FIELDS,
    'artists_tracks': ARTIST_TRACK_FIELDS
}


class CSVWriter:

    def __init__(self):
        self.file_name = 'data_table.csv'
        self.tracks_file = 'tracks.csv'

    def write(self, data, data_type):
        write_rows = rows.get(data_type)
        file_name = self.get_file_name(data_type)
        import os
        mode = 'a' if os.path.isfile(f'./{file_name}') else 'w'
        with open(file_name, mode=mode) as f:
            data_writer = csv.writer(f,
                                     delimiter=',',
                                     quotechar='"',
                                     quoting=csv.QUOTE_MINIMAL)
            if mode == 'w':
                data_writer.writerow(write_rows)
            for row in data:
                data_writer.writerow([row.get(k, '') for k in write_rows])

    def get_file_name(self, data_type):
        return {'artists': self.file_name,
                'artists_tracks': self.tracks_file}.get(data_type)
