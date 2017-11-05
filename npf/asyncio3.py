# https://github.com/aio-libs/aiohttp/issues/2094
import time
import datetime
import asyncio
import aiohttp
import pymysql
import concurrent.futures

fetchLimit = 10000
data = []
ids = []

connection = pymysql.connect(
    "localhost", "root", "root", "scrapy", charset='utf8')
cursor = connection.cursor()


def getLinks():
    sql = "select id,domain,firstLabel,secondLabel,title,link,isExternal from links where isExternal=0 and status !=404 order by domain limit %s" % fetchLimit
    cursor.execute(sql)
    result = cursor.fetchall()
    return result


def storeData():
    stmt = "INSERT INTO tmp_links (domain,firstLabel,secondLabel,title,link,status,hasData,isExternal,crawled) VALUES (%s, %s,%s, %s,%s,%s, %s,%s, %s)"
    cursor.executemany(stmt, data)
    #sql = "update links set crawled=1 where status !=404 and isExternal=0 and crawled=0 limit %s" % fetchLimit
    sql = 'delete from links where id in (' + ','.join(
        map(str, ids)) + ')'
    cursor.execute(sql)
    connection.commit()
    cursor.close()
    connection.close()


async def get(row):

    id, d, firstLabel, secondLabel, title, link, isExternal = row
    isExternal = int(isExternal)
    url = link
    if not isExternal:
        url = d + link
    try:
        headers = {'connection': 'keep-alive'}
        ssl_conn = aiohttp.TCPConnector(
            verify_ssl=False, limit=200, limit_per_host=100)
        async with aiohttp.ClientSession(connector=ssl_conn, headers=headers) as session:
            async with session.get(url) as response:
                # print(response.url)
                status = response.status
                body = len(await response.read())
                item = (d, firstLabel, secondLabel, title,
                        link, status, body, isExternal, 1)
                data.append(item)
                ids.append(id)
                return await response.release()

    except Exception as e:
        print(e)

start = time.time()
loop = asyncio.get_event_loop()
tasks = []

for row in getLinks():
    tasks.append(asyncio.ensure_future(get(row)))

loop.run_until_complete(asyncio.wait(tasks))

print(len(data))
storeData()
end = time.time()
print(end - start)
