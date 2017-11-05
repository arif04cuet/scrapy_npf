from twisted.enterprise import adbapi
import datetime
import pymysql.cursors
import logging
import csv
import validators
import os


class SQLStorePipeline(object):

    def __init__(self):
        self.connection = pymysql.connect(
            "localhost", "root", "root", "scrapy", charset='utf8')
        self.cursor = self.connection.cursor()

    def process_item(self, item, spider):

        data = []
        if item['links']:
            for row in item['links']:

                link = {}
                link['status'] = 200
                link['isExternal'] = 0
                link['hasData'] = 0

                link['link'] = row['link']

                if link['link'].startswith('http'):
                    link['isExternal'] = 1
                elif not link['link'].startswith('/site/'):
                    link['status'] = 404

                
                if link['isExternal'] and not validators.url(link['link']):
                   link['status'] = 404
                elif not link['isExternal'] and not validators.url(row['domain']+link['link']):
                    link['status'] = 404   
                        

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

            stmt = "INSERT INTO tmp_links (domain, firstLabel,secondLabel,title,link,status,hasData,isExternal) VALUES (%s, %s,%s, %s,%s,%s,%s,%s)"
            self.cursor.executemany(stmt, data)

        domain = item['name']
        DomainName = domain
        domain = domain.replace('http://','')
        self.cursor.execute("UPDATE scraped_domains SET crawled=1 WHERE domain='%s' " % (domain))
        self.connection.commit()

        return item

    def close_spider(self, spider):
        self.cursor.close()
        self.connection.close()

    

