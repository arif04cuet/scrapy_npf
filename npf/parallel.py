#!/usr/local/bin/python3.5
import asyncio
import pymysql
from aiohttp import ClientSession
from aiomysql import create_pool

connection = pymysql.connect(
    "localhost", "root", "root", "scrapy", charset='utf8')
cursor = connection.cursor()

fetchLimit = 20000


async def fetch(row, session):

    d, firstLabel, secondLabel, title, link, isExternal = row
    isExternal = int(isExternal)
    url = link
    if not isExternal:
        url = d + link
    
    async with session.get(url) as response:
        print(url)
        return d, firstLabel, secondLabel, title, link, isExternal,len(await response.read()),response.status

async def run(r):
    url = "http://dhaka.gov.bd/"
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
        print(len(responses))

async def go():
    async with create_pool(host='127.0.0.1', port=3306,
                           user='root', password='root',
                           db='scrapy',charset='utf8', loop=loop) as pool:
        async with pool.get() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT 42;")
                value = await cur.fetchone()
                print(value)


def getLinks():
    
    sql = "select domain,firstLabel,secondLabel,title,link,isExternal from links where status !=404 and isExternal=0 and crawled=0 order by id limit %s" % fetchLimit
    cursor.execute(sql)
    result = cursor.fetchall()
    return result

loop = asyncio.get_event_loop()
future = asyncio.ensure_future(run(4))
loop.run_until_complete(future)