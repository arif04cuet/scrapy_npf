import time
import datetime
import asyncio
import aiohttp
import pymysql

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
            data.append(item)


loop = asyncio.get_event_loop()
tasks = []


for row in getLinks():
    tasks.append(asyncio.ensure_future(get(row)))

loop.run_until_complete(asyncio.wait(tasks))
start = time.time()
print(len(data))
end = time.time()
print(end - start)
