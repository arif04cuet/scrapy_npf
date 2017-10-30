#!/usr/local/bin/python3.5
import asyncio
from aiohttp import ClientSession

async def fetch(url, session):
    async with session.get(url) as response:
        return response.status

async def run(r):
    url = "http://dhaka.gov.bd/{}"
    tasks = []

    # Fetch all responses within one Client session,
    # keep connection alive for all requests.
    async with ClientSession() as session:
        try:
            for i in range(r):
                task = asyncio.ensure_future(fetch(url.format(i), session))
                tasks.append(task)

            responses = await asyncio.gather(*tasks)
            # you now have all response bodies in this variable
        except  Exception as e:
            print("%s has error '%s: %s'" % (url, responses.status, responses.reason))
            # now you can decide what you want to do
            # either return the response anyways or do some handling right here
        print(responses)

def print_responses(result):
    print(result)

loop = asyncio.get_event_loop()
future = asyncio.ensure_future(run(100))
loop.run_until_complete(future)