import asyncio
import logging
import aiohttp
import pymysql
import time

fetchLimit = 1000


def getLinks():
    connection = pymysql.connect(
        "localhost", "root", "root", "scrapy", charset='utf8')
    cursor = connection.cursor()
    sql = "select domain,firstLabel,secondLabel,title,link,isExternal from links where status !=404 and isExternal=0 and crawled=0 order by id limit %s" % fetchLimit
    cursor.execute(sql)
    result = cursor.fetchall()
    return result


LOG_FORMAT = ('%(asctime)s %(message)s')
logging.basicConfig(format=LOG_FORMAT)
log = logging.getLogger('root')  # pylint: disable=I0011,C0103
log.setLevel('DEBUG')


async def fetch(session, url):
    "single fetch"
    log.info('trying %s', url)
    try:
        with aiohttp.Timeout(50):
            async with session.get(url, compress=True) as response:
                if response.status == 200:
                    rs = await response.text()
                    log.info('status 200  %s response size %s', url, len(rs))
                else:
                    log.error('received %s for %s', response.status, url)
    except aiohttp.ClientError as exc:
        log.error(exc)
    except asyncio.TimeoutError as exc:
        log.error('fetching data timeout for %s', url)


async def fetch_all(session, urls, loop):
    """This a delegating routine."""
    futures = [loop.create_task(fetch(session, url)) for url in urls]
    for future in asyncio.as_completed(futures):
        result = await future
        if result is not None:
            pass


def main():
    """main code"""
    ssl_conn = aiohttp.TCPConnector(verify_ssl=False)
    urls = [row[0] + row[4] for row in getLinks()]

    loop = asyncio.get_event_loop()
    with aiohttp.ClientSession(loop=loop, connector=ssl_conn) as session:
        loop.run_until_complete(fetch_all(session, urls, loop))


# This is the standard boilerplate that calls the main() function.
if __name__ == '__main__':

    start = time.time()
    main()
    end = time.time()
    print(end - start)
