import aiohttp
import asyncio
import async_timeout
import os
 
@asyncio.coroutine 
def download_coroutine(session, url):
   with aiohttp.Timeout(10):
        resp = yield from session.get(url)
        print(resp.status)
        return (yield from resp.text())
       
 

@asyncio.coroutine 
def main(loop):
    urls = [
        'http://dhaka.gov.bd/site/page/12c324fd-2013-11e7-8f57-286ed488c766',
        'http://dhaka.gov.bd/site/page/4b4a4120-2013-11e7-8f57-286ed488c766'
    ]
 
    with aiohttp.ClientSession(loop=loop) as session:
        tasks = [download_coroutine(session, url) for url in urls]
        yield from asyncio.gather(*tasks)
 
 
if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))