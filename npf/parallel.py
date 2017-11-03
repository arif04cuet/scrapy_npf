#!/usr/local/bin/python3.5
import asyncio
import pymysql
from aiohttp import ClientSession

connection = pymysql.connect(
    "localhost", "root", "root", "scrapy", charset='utf8')
cursor = connection.cursor()

fetchLimit = 1000
data = []


async def fetch(row, session):

    d, firstLabel, secondLabel, title, link, isExternal = row
    isExternal = int(isExternal)
    url = link
    if not isExternal:
        url = d + link

    async with session.get(url) as response:
        # print(url)
        row = (d, firstLabel, secondLabel, title, link, isExternal, len(await response.read()), response.status)
        data.append(row)


async def run(r):

    tasks = []

    # Fetch all responses within one Client session,
    # keep connection alive for all requests.
    async with ClientSession() as session:
        for row in getLinks():
            task = asyncio.ensure_future(fetch(row, session))
            tasks.append(task)

        responses = []
        try:
            responses = await asyncio.gather(*tasks)
        except Exception as e:
            print(e)

        # you now have all response bodies in this variable
        print(len(data))


def getLinks():

    sql = "select domain,firstLabel,secondLabel,title,link,isExternal from links where status !=404 and isExternal=0 and crawled=0 order by id limit %s" % fetchLimit
    cursor.execute(sql)
    result = cursor.fetchall()
    return result


loop = asyncio.get_event_loop()
future = asyncio.ensure_future(run(4))
loop.run_until_complete(future)
