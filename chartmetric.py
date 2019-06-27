import os
from datetime import datetime, timedelta

import requests


class ChartMetricRequestMixin:
    BASE_URL = "https://api.chartmetric.com/api/"
    BASE_HEADERS = {'Content-Type': 'application/json'}

    def _do_request(self, request_method, uri, auth_needed=True, json=None):
        headers = self.get_headers(auth_needed)
        url = self.BASE_URL + uri
        r = requests.request(request_method,
                             url=url,
                             headers=headers,
                             json=json)
        r.raise_for_status()
        return r.json()

    def _get_auth_headers(self):
        raise NotImplementedError

    def get_headers(self, auth_needed=True):
        headers = self.BASE_HEADERS
        if auth_needed:
            headers.update(self._get_auth_headers())
        return headers


class ChartmetricApiBase(ChartMetricRequestMixin):
    def __init__(self):
        self.refresh_token = os.environ.get("CHARTMETRIC_REFRESH_TOKEN", None)
        if not self.refresh_token:
            raise EnvironmentError("Need to set CHARTMETRIC_REFRESH_TOKEN")


class AuthToken(ChartmetricApiBase):
    AUTH_TOKEN_URL = 'token'

    def __init__(self):
        super().__init__()
        self.__token = None
        self.expires_at = None

    def refresh(self):
        return self._get_auth_token()

    def is_valid(self):
        try:
            print( datetime.now() < self.expires_at)
            return datetime.now() < self.expires_at
        except TypeError:
            return False

    def _get_auth_token(self):
        request_data = {
            'refreshtoken': self.refresh_token
        }
        data = self._do_request("post",
                                self.AUTH_TOKEN_URL,
                                auth_needed=False,
                                json=request_data)
        self.__token = data["token"]
        self.expires_at = datetime.now() + timedelta(seconds=data['expires_in'])
        return self.get_token()

    def get_token(self):
        if self.is_valid():
            return self.__token
        return self.refresh()


class ChartmetricClient(ChartMetricRequestMixin):
    TEST_URL = 'artist/2000'
    ARTIST_URL = 'artist/'

    def __init__(self):
        self.token_helper = AuthToken()

    def _get_auth_headers(self):
        return {'Authorization': f'Bearer {self.token}'}

    @property
    def token(self):
        return self.token_helper.get_token()

    def test_request(self):
        return self._do_request("get", self.TEST_URL)

    def _get_artist_url(self, artist_id=''):
        if artist_id:
            return ''.join([self.ARTIST_URL, artist_id])
        return self.ARTIST_URL

    def artists_tracks(self, artist_id):
        if not artist_id:
            raise ValueError('Need to set artist id')
        base = self._get_artist_url(str(artist_id))
        url = '/'.join([base, 'tracks'])
        data = self._do_request("get", url)
        return data
