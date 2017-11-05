
from threading import Thread
import sys
import queue
import requests
import pymysql
import logging
import csv
import os
import grequests
import time
from urllib.parse import urlparse, parse_qs

concurrent = 100
q = queue.Queue(concurrent * 2)

fetchLimit = 100000
data = []
ids = []

connection = pymysql.connect(
    "localhost", "root", "root", "scrapy", charset='utf8')
cursor = connection.cursor()


def getLinks():
    sql = "select id,domain,firstLabel,secondLabel,title,link,isExternal from tmp_links where status !=404 and isExternal=0 order by domain limit %s" % fetchLimit
    cursor.execute(sql)
    result = cursor.fetchall()
    return result

def copy404rows():
    sql = 'insert into links (domain,firstLabel,secondLabel,title,link,status,hasData,isExternal) select domain,firstLabel,secondLabel,title,link,status,hasData,isExternal from links where status=404'
    cursor.execute(sql)
    sql = 'delete from tmp_links where status=404'
    cursor.execute(sql)


def storeData():
    
    if data:
        stmt = "INSERT INTO links (domain,firstLabel,secondLabel,title,link,status,hasData,isExternal) VALUES (%s, %s,%s, %s,%s,%s, %s,%s)"
        cursor.executemany(stmt, data)

    if ids:
        sql = 'delete from tmp_links where id in (' + ','.join(
            map(str, ids)) + ')'
        cursor.execute(sql)
        connection.commit()




def doWork():
    while True:
        row = q.get()
        if len(row) == 7:
            id, d, firstLabel, secondLabel, title, link, isExternal = row
            isExternal = int(isExternal)
            url = link
            if not isExternal:
                url = d + link

            status, hd = getStatus(d, url, isExternal)
            item = (d, firstLabel, secondLabel, title,
                        link, status, hd, isExternal, 1)

            data.append(item)
            ids.append(id)
            print(len(ids))
            if(len(ids) == fetchLimit-1):
                storeData()
                data.clear()
                ids.clear()
        else:
            print(row)

        q.task_done()


def getStatus(d, url, isExternal):
    
    try:
        time.sleep(0.05)
        if isExternal:
            res = requests.head(url)
        else:
            res = requests.get(url)

        return res.status_code, len(res.content)

    except requests.exceptions.RequestException as e:
        return 0, 0


def startTaskParallal(dataLIst):
    concurrent_limit = 100
    hdrs = {'connection' : 'keep-alive'}
    urls = []
    tmp_data = {}
    for row in dataLIst:
        id, d, firstLabel, secondLabel, title, link, isExternal = row
        
        isExternal = int(isExternal)
        url = link
        if not isExternal:
            url = d + link
            
        urls.append([id,url,isExternal])
        tmp_data[id] = row


    rs = (grequests.head(u[1],allow_redirects=False,params={'uniqueid':u[0]}) if u[2] else grequests.get(u[1],allow_redirects=False,params={'uniqueid':u[0]}) for u in urls)
    #res = grequests.map(rs)
    for res in grequests.imap(rs,size=concurrent_limit):

        if res is not None:
            url = res.url
            print(url)
            params = parse_qs(urlparse(url).query)
            
            if 'uniqueid' in params:
                key = int(params['uniqueid'][0])
                
                id, d, firstLabel, secondLabel, title, link, isExternal = tmp_data[key]
                
                data.append((d, firstLabel, secondLabel, title,
                            link, res.status_code, len(res.content), isExternal))
                ids.append(id)
                #print(len(ids))  


def startTask(dataLIst):

    for i in range(concurrent):
        t = Thread(target=doWork)
        t.daemon = True
        t.start()
    try:

        for row in dataLIst:
            q.put(row)
        q.join()

        #insertUpdate(data)
        #data.clear()

    except KeyboardInterrupt:
        sys.exit(1)


start = time.time()
copy404rows()

for i in range(0, 1):
    rows = getLinks()
    startTaskParallal(rows)
    storeData()

cursor.close()
connection.close()
end = time.time()
print(end - start)