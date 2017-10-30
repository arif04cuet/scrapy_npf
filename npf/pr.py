import grequests
import requests
import pymysql

connection = pymysql.connect("localhost","root","root","scrapy",charset='utf8')
cursor = connection.cursor()
sql = "select domain,firstLabel,secondLabel,title,link,isExternal from links where status !=404 and crawled=0 order by domain,isExternal limit 10";
cursor.execute(sql)
result=cursor.fetchall()
urls = []
for row in result:
    d,firstLabel,secondLabel,title,url,isExternal = row
    isExternal = int(isExternal)
    if not isExternal:
       url = d+url
    urls.append(url)

rs = (grequests.get(u) for u in urls)
res = grequests.map(rs)
for r in res:
    if r is not None:
        #print(len(r.content))
        print(r.status_code)

# connection = pymysql.connect("localhost","root","root","scrapy",charset='utf8')
# cursor = connection.cursor()
# sql = "select domain,firstLabel,secondLabel,title,link,isExternal from links where status !=404 and crawled=0 order by domain,isExternal limit 100000";
# cursor.execute(sql)
# result=cursor.fetchall()

# for row in result:
#             q.put(row)
# q.join()

# #print(data)

# stmt = "INSERT INTO tmp_links (domain,firstLabel,secondLabel,title,link,status,hasData,isExternal,crawled) VALUES (%s, %s,%s, %s,%s,%s, %s,%s, %s)"
# cursor.executemany(stmt, data)
# connection.commit()