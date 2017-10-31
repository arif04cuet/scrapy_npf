
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

concurrent = 500
q = queue.Queue(concurrent * 2)
data = []
connection = pymysql.connect(
    "localhost", "root", "root", "scrapy", charset='utf8')
cursor = connection.cursor()

fetchLimit = 5000


def doWork():
    while True:
        row = q.get()
        if len(row) == 6:
            d, firstLabel, secondLabel, title, url, isExternal = row
            isExternal = int(isExternal)
            status, hd, url = getStatus(d, url, isExternal)
            #doSomethingWithResult(d, firstLabel, secondLabel,
            #                      title, url, status, hd, isExternal)
        else:
            print(row)

        q.task_done()


def getStatus(d, ourl, isExternal):
    try:

        url = ourl
        if not isExternal:
            url = d + ourl

        if isExternal:
            res = requests.head(url)
        else:
            res = requests.get(url)
        print (url)
        return res.status_code, len(res.content), ourl

    except requests.exceptions.RequestException as e:
        return 0, 0, ourl


def doSomethingWithResult(d, firstLabel, secondLabel, title, url, status, hd, isExternal):

    data.append((d, firstLabel, secondLabel, title,
                url, status, hd, isExternal, 1))


def startTaskParallal(dataLIst):
    
    urls = []
    for row in dataLIst:
        d, firstLabel, secondLabel, title, url, isExternal = row
        isExternal = int(isExternal)
        if not isExternal:
            url = d + url
        urls.append(url)

    rs = (grequests.get(u) for u in urls)
    #res = grequests.map(rs)
    for res in grequests.imap(rs):

        if res is not None:
            url = res.url
            print (url)
            data.append((d, firstLabel, secondLabel, title,
                         url, res.status_code, len(res.content), isExternal, 1))

    insertUpdate(data)
    data.clear()


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


def insertUpdate(dataSet):
    stmt = "INSERT INTO tmp_links (domain,firstLabel,secondLabel,title,link,status,hasData,isExternal,crawled) VALUES (%s, %s,%s, %s,%s,%s, %s,%s, %s)"
    cursor.executemany(stmt, dataSet)
    sql = "update links set crawled=1 where status !=404 and isExternal=0 and crawled=0 limit %s" % fetchLimit
    cursor.execute(sql)
    connection.commit()


def getLinks():
    start = time.time()
    sql = "select domain,firstLabel,secondLabel,title,link,isExternal from links where status !=404 and isExternal=0 and crawled=0 order by id limit %s" % fetchLimit
    cursor.execute(sql)
    result = cursor.fetchall()
    startTaskParallal(result)
    end = time.time()
    print(end - start)


for i in range(0, 1):
    getLinks()
