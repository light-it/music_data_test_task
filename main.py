import asyncio
from datetime import datetime

from async_client import async_client
from csv_writer import CSVWriter

ARTISTS_NEEDED = 6000


class FanStatsCollector:
    def __init__(self, artist_id, client):
        self.artist_id = artist_id
        self.client = client

    async def spotify(self):
        stats = dict()
        spotify_fan_stats = await self.client.artist_fan_stats(self.artist_id)
        total_spotify_followers = 0
        if not spotify_fan_stats:
            return stats
        for follower in spotify_fan_stats['obj']['followers']:
            try:
                total_spotify_followers += follower['value']
            except TypeError:
                continue

        if total_spotify_followers:
            avg_spotify_followers = (total_spotify_followers /
                                     len(spotify_fan_stats['obj']['followers']))
            stats['avg_spotify_followers'] = avg_spotify_followers
            return stats

    async def facebook(self):
        stats = dict()
        facebook_fan_stats = await self.client.artist_fan_stats(
            self.artist_id, source='facebook')
        if not facebook_fan_stats:
            return stats
        # fb likes
        total_fb_likes = 0
        for like in facebook_fan_stats['obj']['likes']:
            try:
                total_fb_likes += int(like['value'])
            except TypeError:
                continue
        if total_fb_likes:
            avg_fb_likes = total_fb_likes / len(
                facebook_fan_stats['obj']['likes'])
            stats['avg_fb_likes'] = avg_fb_likes

        # fb talks
        total_fb_talks = 0
        for talk in facebook_fan_stats['obj']['talks']:
            try:
                total_fb_talks += int(talk['value'])
            except TypeError:
                continue

        # fb engagement
        total_engagement = total_fb_talks + total_fb_likes
        if total_engagement:
            if total_fb_likes:
                avg_fb_engagement = total_engagement / len(
                    facebook_fan_stats['obj']['likes'])
            else:
                avg_fb_engagement = total_engagement / len(
                    facebook_fan_stats['obj']['talks'])
            stats['avg_fb_engagement'] = avg_fb_engagement
        return stats

    async def youtube(self):
        stats = dict()
        youtube_fan_stats = await self.client.artist_fan_stats(
            self.artist_id, source='youtube_channel')
        total_youtube_subscribers = 0
        if not youtube_fan_stats:
            return stats
        for subscriber in youtube_fan_stats['obj']['subscribers']:
            try:
                total_youtube_subscribers += int(subscriber['value'])
            except TypeError:
                continue

        if total_youtube_subscribers:
            avg_youtube_subscribers = (total_youtube_subscribers /
                                       len(youtube_fan_stats['obj'][
                                               'subscribers']))
            stats['avg_youtube_subscribers'] = avg_youtube_subscribers

        total_youtube_comments = 0
        for comments in youtube_fan_stats['obj']['comments']:
            try:
                total_youtube_comments += int(comments['value'])
            except TypeError:
                continue

        total_youtube_views = 0
        for view in youtube_fan_stats['obj']['views']:
            try:
                total_youtube_views += int(view['value'])
            except TypeError:
                continue

        youtube_engagement = total_youtube_views + total_youtube_comments
        if youtube_engagement:
            if total_youtube_views:
                avg_youtube_engagement = (youtube_engagement /
                                          len(youtube_fan_stats['obj'][
                                                  'views']))
            else:
                avg_youtube_engagement = (youtube_engagement /
                                          len(youtube_fan_stats['obj'][
                                                  'comments']))

            stats['avg_youtube_engagement'] = avg_youtube_engagement
        return stats


class TracksCollector:
    def __init__(self, artist_id, client):
        self.artist_id = artist_id
        self.client = client

    async def tracks_list(self):
        tracks = await self.client.artist_tracks(self.artist_id)
        if tracks:
            return tracks['obj']


async def collect_fan_stats(artist_id, client):
    stats = dict()
    fan_stats_collector = FanStatsCollector(artist_id, client)
    spotify = await fan_stats_collector.spotify()
    facebook = await fan_stats_collector.facebook()
    youtube = await fan_stats_collector.youtube()
    responses = [spotify, facebook, youtube]
    for item in responses:
        if item:
            stats.update(item)
    return stats


async def get_artists_data(client):
    queries_count = int(ARTISTS_NEEDED / 200)
    offset = 0
    csv_writer = CSVWriter()
    for idx in range(queries_count):
        artists_list = list()
        resp = await client.artists_list(offset=offset+200*idx)
        for artist in resp['obj']['data']:
            artist_id = artist['chartmetric_artist_id']
            fan_stats = await collect_fan_stats(artist_id, client)
            artist.update(fan_stats)
            tracks_helper = TracksCollector(artist_id, client)
            tracks_list = await tracks_helper.tracks_list()
            tracks = []
            for track in tracks_list:
                tmp_dict = dict(artist_id=artist_id)
                if 'id' in track:
                    tmp_dict['track_id'] = track['id']
                else:
                    tmp_dict['track_id'] = track['track_id']
                tmp_dict['name'] = track['name']
                if track['release_dates']:
                    tmp_dict['release_dates'] = ' '.join(
                        [d if d else '' for d in track['release_dates']])
                tracks.append(tmp_dict)
            csv_writer.write(tracks, 'artists_tracks')
            artists_list.append(artist)

        print(f'Took {offset+200*idx} offset')
        csv_writer.write(artists_list, 'artists')
    await client.close()


if __name__ == '__main__':
    started = datetime.now()
    try:
        client = async_client.AsyncChartMetric()
        loop = asyncio.get_event_loop()
        artists_future = asyncio.ensure_future(get_artists_data(client))
        loop.run_until_complete(artists_future)

        loop.close()
    except Exception as e:
        raise e
    finally:
        elapsed = datetime.now() - started
        print('elapsed = ', elapsed)
