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

        stmt = "INSERT INTO links (domain, firstLabel,secondLabel,title,link,status,hasData,isExternal) VALUES (%s, %s,%s, %s,%s,%s,%s,%s)"
        self.cursor.executemany(stmt, data)
        domain = data[0][0]
        DomainName = domain
        domain = domain.replace('http://','')
        self.cursor.execute("UPDATE scraped_domains SET crawled=1 WHERE domain='%s' " % (domain))
        self.connection.commit()
        #open('domain_list.csv', 'w',encoding='utf-8')
        # with open('domain_list.csv', 'a+',encoding='utf-8') as csvfile:
        #     writer = csv.writer(csvfile, delimiter='$')
        #     self.cursor.execute("select id,domain,link,status,hasData,isExternal from links WHERE status !=404 and domain='%s' " % (DomainName))
        #     result=self.cursor.fetchall()
        #     for i in result:
        #         writer.writerow(i)

        # # logging.info(data)
        # self.connection.commit()
        #os.system('python cr.py')
        return item

    def close_spider(self, spider):
        self.cursor.close()
        self.connection.close()

    

