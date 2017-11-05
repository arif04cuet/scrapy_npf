
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

CONCURRENT = 100

fetchLimit = 200
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



q = queue.Queue(CONCURRENT * 2)
class MyAwesomeThread(Thread):
    """
    Threading wrapper to handle counting and processing of tasks
    """
    def __init__(self, session, q):
        self.q = q
        self.count = 0
        self.session = session
        self.response = None
        Thread.__init__(self)

    def getStatus(self,response):
        
        try:
            return response.status_code, len(response.content)
        except requests.exceptions.RequestException as e:
            return 0, 0

    def run(self): 
        
        """TASK RUN BY THREADING"""
        while True:
            id, d, firstLabel, secondLabel, title, link, isExternal = row
            isExternal = int(isExternal)
            url = link
            if not isExternal:
                url = d + link
            try:
                httpHeaders = {'connection' : 'keep-alive'}
                print(self.count)
                self.response = self.session.get(url)
                print(self.response)
                status, hd = self.getStatus(self.response)
                item = (d, firstLabel, secondLabel, title,
                            link, status, hd, isExternal, 1)

                data.append(item)
                ids.append(id)
                print(data)
    
                # handle response here
                self.count+= 1
                self.q.task_done()
            except Exception as e:
                print(e)    

        #return


start = time.time()
threads = []
for i in range(CONCURRENT):
    session = requests.session()
    t=MyAwesomeThread(session,q)
    t.daemon=True # allows us to send an interrupt 
    threads.append(t)


## build urls and add them to the Queue
for row in getLinks():
    q.put_nowait(row)

## start the threads
for t in threads:
    t.start()

#storeData()

cursor.close()
connection.close()
end = time.time()
print(end - start)