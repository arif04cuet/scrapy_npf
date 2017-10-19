#!/usr/bin/python3

import pymysql

# Open database connection
db = pymysql.connect("localhost","root","root","scrapy" )

# prepare a cursor object using cursor() method
cursor = db.cursor()

# execute SQL query using execute() method.
sql = "select * from links";
cursor.execute(sql)
result=cursor.fetchall()

file = open("urllist.txt", "w")
urls = []
for row in result:
    url = row[5]
    if not url.startswith('http'):
        url = row[1].strip()+url
    urls.append(url)
    file.write(url+'\n')

file.close()

# disconnect from server
db.close()