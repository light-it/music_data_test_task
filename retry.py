import asyncio
import json

import aiohttp

HTTP_STATUS_CODES_TO_RETRY = (500, 502, 503, 504, 429)


class FailedRequest(Exception):
    """
    A wrapper of all possible exception during a HTTP request
    """
    code = 0
    message = ''
    url = ''
    raised = ''

    def __init__(self, *, raised='', message='', code='', url=''):
        self.raised = raised
        self.message = message
        self.code = code
        self.url = url

        super().__init__("code:{c} url={u} message={m} raised={r}".format(
            c=self.code, u=self.url, m=self.message, r=self.raised))


async def send_http(session, method, url, *,
                    retries=-1,
                    interval=60,
                    backoff=3,
                    http_status_codes_to_retry=HTTP_STATUS_CODES_TO_RETRY,
                    **kwargs):
    """
    Sends a HTTP request and implements a retry logic.

    Arguments:
        session (obj): A client aiohttp session object
        method (str): Method to use
        url (str): URL for the request
        retries (int): Number of times to retry in case of failure
        interval (float): Time to wait before retries
        backoff (int): Multiply interval by this factor after each failure
        read_timeout (float): Time to wait for a response
    """
    backoff_interval = interval
    raised_exc = None
    attempt = 0
    method = method.lower()
    if method not in ['get', 'patch', 'post']:
        raise ValueError

    if retries == -1:  # -1 means retry indefinitely
        attempt = -1
    elif retries == 0: # Zero means don't retry
        attempt = 1
    else:  # any other value means retry N times
        attempt = retries + 1

    while attempt != 0:
        if raised_exc:
            print('WAITING !!!')
            await asyncio.sleep(backoff_interval)
            # bump interval for the next possible attempt
            backoff_interval = backoff_interval * backoff
        try:
            async with getattr(session, method)(url, **kwargs) as response:
                print(f"sending  - > {url}")
                if response.status == 200:
                    data = await response.json()
                    return data
                elif response.status in http_status_codes_to_retry:
                    print('retrying')
                    raise aiohttp.ClientResponseError(
                        code=response.status,
                        message=response.reason,
                        history=response._history,
                        request_info=response._request_info)
                else:
                    try:
                        data = await response.json()
                    except json.decoder.JSONDecodeError as exc:
                        raise FailedRequest(
                            code=response.status, message=str(exc),
                            raised=exc.__class__.__name__, url=url)
                    else:
                        print('received %s for %s', data, url)
                        print(data['errors'][0]['detail'])
                        raised_exc = None
        except (aiohttp.ClientResponseError,
                # aiohttp.ClientRequestError,
                asyncio.TimeoutError) as exc:
            try:
                code = exc.code
            except AttributeError:
                code = ''
            raised_exc = FailedRequest(code=code, message=exc, url=url,
                                       raised=exc.__class__.__name__)
        else:
            raised_exc = None
            break

        attempt -= 1

    if raised_exc:
        raise raised_exc
