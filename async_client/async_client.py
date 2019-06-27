import asyncio
import aiohttp
import json
import os
from datetime import datetime, timedelta, date

from yarl import URL

from retry import send_http
from .utils import httpize


class AsyncChartMetric:
    AUTH_TOKEN_URL = 'token'

    def __init__(self):
        self.refresh_token = os.environ.get("CHARTMETRIC_REFRESH_TOKEN", None)
        if not self.refresh_token:
            raise EnvironmentError("Need to set CHARTMETRIC_REFRESH_TOKEN")
        self.base_url = 'https://api.chartmetric.com/api'
        self.session = aiohttp.ClientSession()
        self.token_expires_at = None
        self.__auth_token = None

    def auth_token_is_valid(self):
        try:
            return datetime.now() < self.token_expires_at
        except TypeError:
            return False

    async def get_auth_headers(self):
        await self.get_token()
        return {'Authorization': f'Bearer {self.__auth_token}'}

    async def get_token(self):
        if self.auth_token_is_valid():
            return self.__auth_token
        await self.refresh()

    async def refresh(self):
        request_data = {
            'refreshtoken': self.refresh_token
        }
        data = await self._query_json(
            path=self.AUTH_TOKEN_URL,
            auth_required=False,
            method="POST",
            data=request_data)
        self.__auth_token = data["token"]
        self.token_expires_at = datetime.now() + timedelta(seconds=data['expires_in'])

    async def close(self):
        await self.session.close()

    def _canonicalize_url(self, path):
        return URL(f"{self.base_url}/{path}")

    async def _query(self, path, method="GET", *, params=None, data=None,
                     headers=None, auth_required=True, timeout=None, chunked=None):
        url = self._canonicalize_url(path)
        headers = headers or {}
        if headers and "content-type" not in headers:
            headers["content-type"] = "application/json"
        try:

            response = await send_http(self.session, method,
                                       url,
                                       params=httpize(params),
                                       headers=headers,
                                       data=data,
                                       timeout=timeout,
                                       chunked=chunked)

        except asyncio.TimeoutError:
                raise
        return response

    async def _query_json(self, path, method="GET", *, params=None, data=None,
                          headers=None, auth_required=True, timeout=None):
        if headers is None:
            headers = {}
        headers["content-type"] = "application/json"
        if not isinstance(data, (str, bytes)):
            data = json.dumps(data)
        response = await self._query(
            path,
            method,
            params=params,
            headers=headers,
            data=data,
            auth_required=auth_required,
            timeout=timeout
        )
        return response

    async def test_request(self):
        headers = await self.get_auth_headers()
        response = await self._query(
            path='artist/2000',
            method="GET",
            headers=headers)
        data = await response.json()
        print(data)

    async def artists_list(self,
                           min=50,
                           max=100,
                           query_type="sp_popularity",
                           offset=0):
        headers = await self.get_auth_headers()
        path = f'artist/{query_type}/list?min={min}&max={max}&offset={offset}'

        response = await self._query(
            path=path,
            method="GET",
            headers=headers,
        )
        return response

    async def artist_meta(self, artist_id):
        if not artist_id:
            return
        headers = await self.get_auth_headers()
        path = f'artist/{artist_id}'
        response = await self._query(
            path=path,
            method="GET",
            headers=headers,
        )
        # response = await response.json()
        return response

    async def artist_fan_stats(self, artist_id, source='spotify', field=None):
        if not artist_id:
            return

        end = date.today().replace(day=1) - timedelta(days=1)
        start = end.replace(day=1)

        headers = await self.get_auth_headers()

        path = f'artist/{artist_id}/stat/{source}?since={start}&until={end}'
        if field:
            path += f'&field={field}'
        response = await self._query(
            path=path,
            method="GET",
            headers=headers,
        )
        return response

    async def artist_tracks(self, artist_id):
        if not artist_id:
            return None

        headers = await self.get_auth_headers()
        path = f'artist/{artist_id}/tracks'
        response = await self._query(
            path=path,
            method="GET",
            headers=headers,
        )
        # data = await response.json()
        return response

    async def track_detail(self, track_id):
        if not track_id:
            return

        headers = await self.get_auth_headers()
        path = f'track/{track_id}'
        response = await self._query(
            path=path,
            method="GET",
            headers=headers
        )
        return response
