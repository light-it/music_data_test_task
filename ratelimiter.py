#!/usr/bin/env python3

import asyncio
import time

import aiohttp

START = time.monotonic()


class RateLimiter:
    RATE = 1
    MAX_TOKENS = 1

    def __init__(self, client):
        self.client = client
        self.tokens = self.MAX_TOKENS
        self.updated_at = time.monotonic()

    async def request(self, *args, **kwargs):
        await self.wait_for_token()
        now = time.monotonic() - START
        print(f'{now:.0f}s: ask {args[1]}')
        return await self.client.request(*args, **kwargs)

    async def wait_for_token(self):
        while self.tokens < 1:
            self.add_new_tokens()
            await asyncio.sleep(0.1)
        self.tokens -= 1

    async def close(self):
        await self.client.close()

    def add_new_tokens(self):
        now = time.monotonic()
        time_since_update = now - self.updated_at
        new_tokens = time_since_update * self.RATE
        if self.tokens + new_tokens >= 1:
            self.tokens = min(self.tokens + new_tokens, self.MAX_TOKENS)
            self.updated_at = now


async def fetch_one(client, i):
        url = f'https://httpbin.org/get?i={i}'
        # Watch out for the extra 'await' here!
        async with await client.get(url) as resp:
            resp = await resp.json()
            now = time.monotonic() - START
            print(f"{now:.0f}s: got {resp['args']}")


async def main():
    async with aiohttp.ClientSession() as client:
        client = RateLimiter(client)
        tasks = [asyncio.ensure_future(fetch_one(client, i)) for i in range(20)]
        await asyncio.gather(*tasks)


if __name__ == '__main__':
    # Requires Python 3.7+
    asyncio.run(main())
