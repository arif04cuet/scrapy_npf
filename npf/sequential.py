import asyncio
import aiohttp
from datetime import datetime

statuses = []


@asyncio.coroutine
def get(url):
    print('[{}] Doing GET request to {}'.format(datetime.now().strftime('%H:%M:%S:%f'), url))
    response = yield from aiohttp.request('GET', url)
    statuses.append({'url': url, 'status': response.status})
    response.close()


@asyncio.coroutine
def main():
    urls = ['http://google.com', 'http://facebook.com', 'http://twitter.com', 'http://slack.com']
    for url in urls:
        yield from get(url)

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
loop.close()

print('****** STATUSES:', statuses)
