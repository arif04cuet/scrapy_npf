from urllib.parse import urlparse
from urllib.parse import urlencode
from threading import Thread
import http.client
import sys
import queue
import requests
import pymysql
import logging
import csv
import os;

concurrent = 500
data = []

def doWork():
    while True:
        row = q.get()
        if len(row) == 6:
            d,firstLabel,secondLabel,title,url,isExternal = row
            isExternal = int(isExternal)
            status,hd,url = getStatus(d,url,isExternal)
            doSomethingWithResult(d,firstLabel,secondLabel,title,url,status,hd,isExternal)
        else:
            print(row)

        q.task_done()


def getStatus(d,ourl,isExternal):
    try:

        url = ourl
        if not isExternal:
           url = d+ourl
            
        if isExternal:
            res = requests.head(url)
        else:
            res = requests.get(url)    
        print (url)
        return res.status_code, len(res.content), ourl

    except requests.exceptions.RequestException as e:
        return 0, 0, ourl

def doSomethingWithResult(d,firstLabel,secondLabel,title,url,status,hd,isExternal):
    
     data.append((d,firstLabel,secondLabel,title,url,status,hd,isExternal,1))
   


q = queue.Queue(concurrent * 2)
for i in range(concurrent):
    t = Thread(target=doWork)
    t.daemon = True
    t.start()
try:
    connection = pymysql.connect("localhost","root","root","scrapy",charset='utf8')
    cursor = connection.cursor()

    


    sql = "select domain,firstLabel,secondLabel,title,link,isExternal from links where status !=404 and crawled=0 order by domain,isExternal limit 5000";
    cursor.execute(sql)
    result=cursor.fetchall()
    
    for row in result:
             q.put(row)
    q.join()

    #print(data)

    stmt = "INSERT INTO tmp_links (domain,firstLabel,secondLabel,title,link,status,hasData,isExternal,crawled) VALUES (%s, %s,%s, %s,%s,%s, %s,%s, %s)"
    cursor.executemany(stmt, data)

    sql = "update links set crawled=1 where crawled=0 limit 5000";
    cursor.execute(sql)
    connection.commit()
    
except KeyboardInterrupt:
    sys.exit(1)
