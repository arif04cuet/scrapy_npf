import time
import datetime
import asyncio
import aiohttp
import pymysql
import concurrent.futures
import requests

fetchLimit = 1000
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


def get(row):

    id, d, firstLabel, secondLabel, title, link, isExternal = row
    isExternal = int(isExternal)
    url = link
    if not isExternal:
        url = d + link
    try:

        response = requests.get(url)
        print(response.url)
        status = response.status_code
        body = len(response.content)
        item = (d, firstLabel, secondLabel, title,
                link, status, body, isExternal, 1)
        data.append(item)
        ids.append(id)

    except Exception as e:
        print(e)


async def main():

    with concurrent.futures.ThreadPoolExecutor(max_workers=200) as executor:
        loop = asyncio.get_event_loop()
        futures = [
            loop.run_in_executor(
                None,
                get,
                i
            )
            for i in getLinks()
        ]

        start = time.time()
        print(len(await asyncio.gather(*futures)))
        storeData()
        end = time.time()
        print(end - start)


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
