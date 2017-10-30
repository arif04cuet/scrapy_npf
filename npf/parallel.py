import asyncio
import aiohttp
from datetime import datetime
import pymysql
statuses = []


@asyncio.coroutine
def get(url):
    
    try:
        print('[{}] Doing GET request to {}'.format(datetime.now().strftime('%H:%M:%S:%f'), url)) 
        response = yield from aiohttp.request('GET', url)
        statuses.append({'url': url, 'length':response.headers.get('Content-Length'), 'status': response.status})
    except Exception as e:
        print("%s" % (e))
            # now you can decide what you want to do
            # either return the response anyways or do some handling right here
    #response.close()


connection = pymysql.connect("localhost","root","root","scrapy",charset='utf8')
cursor = connection.cursor()
sql = "select domain,firstLabel,secondLabel,title,link,isExternal from links where status !=404 and crawled=0 order by domain,isExternal limit 1000";
cursor.execute(sql)
result=cursor.fetchall()
urls = []
for row in result:
    d,firstLabel,secondLabel,title,url,isExternal = row
    isExternal = int(isExternal)
    if not isExternal:
       url = d+url
    urls.append(url)

tasks = [asyncio.Task(get(url)) for url in urls]

loop = asyncio.get_event_loop()
loop.run_until_complete(asyncio.gather(*tasks))
loop.close()

print('****** STATUSES:', statuses)
