import time
import datetime
import asyncio
import aiohttp
import pymysql
import requests
import concurrent.futures

fetchLimit = 50
data = []


def formatUrl(url):
    return url + '?run={}'.format(time.time())


def getLinks():
    connection = pymysql.connect(
        "localhost", "root", "root", "scrapy", charset='utf8')
    cursor = connection.cursor()
    sql = "select domain,firstLabel,secondLabel,title,link,isExternal from links where status !=404 and isExternal=0 and crawled=0 order by id limit %s" % fetchLimit
    cursor.execute(sql)
    result = cursor.fetchall()
    return result


def storeData(row):
    data.append(row)


async def get(row):

    d, firstLabel, secondLabel, title, link, isExternal = row
    isExternal = int(isExternal)
    url = link
    if not isExternal:
        url = d + link

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            print(response.url)
            status = response.status
            body = len(await response.read())
            item = (d, firstLabel, secondLabel, title,
                    link, status, body, isExternal, 1)
            await storeData(item)


async def main():

    loop = asyncio.get_event_loop()
    futures = [
        loop.run_in_executor(
            None,
            requests.get,
            i[0] + i[4]
        )
        for i in getLinks()
    ]

    start = time.time()
    print(len(await asyncio.gather(*futures)))
    end = time.time()
    print(end - start)


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
