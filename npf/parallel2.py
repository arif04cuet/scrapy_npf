import aiohttp
import asyncio
import async_timeout
import os
import pymysql
import time

fetchLimit = 300
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


@asyncio.coroutine 
def download_coroutine(session, row):
    id, d, firstLabel, secondLabel, title, link, isExternal = row
    isExternal = int(isExternal)
    url = link
    if not isExternal:
        url = d + link 
       
    with aiohttp.Timeout(20):
        try:
            
            resp = yield from session.get(url)
            status =resp.status
            body = yield from resp.text()
            item = (d, firstLabel, secondLabel, title,
                            link, status, len(body), isExternal, 1)
            print(url)
            data.append(item)
            ids.append(id)
        except Exception as e:
            print(e)


@asyncio.coroutine 
def main(loop):
 
    with aiohttp.ClientSession(loop=loop) as session:
        tasks = [download_coroutine(session, row) for row in getLinks()]
        yield from asyncio.gather(*tasks)
 
 
if __name__ == '__main__':
    start = time.time()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
    print(len(data))
    storeData()
    end = time.time()
    print(end - start)
