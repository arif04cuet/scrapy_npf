from urllib.parse import urlparse
from threading import Thread
import http.client
import sys
import queue
import requests

concurrent = 200


def doWork():
    while True:
        url = q.get()
        resp, read, url = getStatus(url)
        doSomethingWithResult(resp, read, url)
        q.task_done()


def getStatus(ourl):
    try:
        url = urlparse(ourl)
        conn = http.client.HTTPConnection(url.netloc)
        conn.request("HEAD", url.path)
        res = conn.getresponse()
        return res.status, len(res.read()), ourl
    except:
        return "error", ourl


def getInfo(url):
    try:
        res = requests.head(url)
        return res.status_code, res.headers['Content-Length'], url
    except:
        return "error", url


def doSomethingWithResult(resp, read, url):
    print (resp, read, url)


q = queue.Queue(concurrent * 2)
for i in range(concurrent):
    t = Thread(target=doWork)
    t.daemon = True
    t.start()
try:
    for url in open('urllist.txt'):
        q.put(url.strip())
    q.join()
except KeyboardInterrupt:
    sys.exit(1)
