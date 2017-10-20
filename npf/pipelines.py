# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html


# import sys
# import MySQLdb
# import hashlib
# from scrapy.exceptions import DropItem
# from scrapy.http import Request


# class NpfPipeline(object):
#     def __init__(self):
#         self.conn = MySQLdb.connect(
#             'localhost', 'root', 'root', 'scrapy', charset="utf8", use_unicode=True)
#         self.cursor = self.conn.cursor()


# def process_item(self, item, spider):
#     try:
#         self.cursor.execute("""INSERT INTO example_book_store (book_name, price)
#                         VALUES (%s, %s)""",
#                             (item['book_name'].encode('utf-8'),
#                              item['price'].encode('utf-8')))

#         self.conn.commit()

#     except MySQLdb.Error, e:
#         print "Error %d: %s" % (e.args[0], e.args[1])

#     return item


from twisted.enterprise import adbapi
import datetime
import pymysql.cursors
import logging


class SQLStorePipeline(object):

    def __init__(self):
        self.connection = pymysql.connect(
            "localhost", "root", "root", "scrapy", charset='utf8')
        self.cursor = self.connection.cursor()

    def process_item(self, item, spider):

        data = []
        for row in item['links']:

            link = {}
            link['status'] = 200
            link['isExternal'] = 0
            link['hasData'] = 1

            link['link'] = row['link']

            if link['link'].startswith('http'):
                link['isExternal'] = 1
                link['status'] = 200
            elif not link['link'].startswith('/site/'):
                link['status'] = 404
            else:
                link['status'] = 200

            data.append((
                row['domain'],
                row['firstLabel'],
                row['secondLabel'],
                row['title'],
                row['link'],
                link['status'],
                link['hasData'],
                link['isExternal']
            ))

        stmt = "INSERT INTO links (domain, firstLabel,secondLabel,title,link,status,hasData,isExternal) VALUES (%s, %s,%s, %s,%s,%s,%s,%s)"
        self.cursor.executemany(stmt, data)
        self.connection.commit()
        # logging.info(data)
        return item

    def close_spider(self, spider):
        self.cursor.close()
        self.connection.close()
