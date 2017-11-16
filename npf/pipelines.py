from twisted.enterprise import adbapi
import datetime
import pymysql.cursors
import logging
import csv
import validators

from urllib.parse import urlparse, parse_qs

class NpfExternalPipeline(object):
    
    data = []

    def process_item(self, item, spider):

        
        self.data.append(
            (
                
                item['link'],
                item['status'],
                1
               
            )
        )

        print(len(self.data))    
        return item

    def close_spider(self, spider):
        
        self.data = self.data + spider.errors_data
        
        connection = pymysql.connect(
            "localhost", "root", "root", "scrapy", charset='utf8')
        cursor = connection.cursor()
      
        if self.data:
            stmt = "INSERT INTO unique_links (link,status,isExternal) VALUES (%s, %s,%s)"
            cursor.executemany(stmt, self.data)

        sql = 'update `tmp_links` tl join unique_links ul on tl.link=ul.link set tl.status=ul.status'
        cursor.execute(sql)

        sql = 'insert into links (domain,firstLabel,secondLabel,title,link,status,hasData,isExternal) select domain,firstLabel,secondLabel,title,link,status,hasData,isExternal from tmp_links'
        cursor.execute(sql)
        
        sql = 'delete from tmp_links where isExternal=1'
        cursor.execute(sql)
        
        connection.commit()
        cursor.close()
        connection.close()
        

class Npf2Pipeline(object):
    
    data = []
    ids = []

    def __init__(self):
        pass
       
        
    def process_item(self, item, spider):

        
        self.data.append(
            (
                item['domain'],
                item['firstLabel'],
                item['secondLabel'],
                item['title'],
                item['link'],
                item['status'],
                item['hasData'],
                item['isExternal']
            )
        )

        self.ids.append(item['id'])

        print(len(self.data))    
        return item

    def close_spider(self, spider):
        
        self.ids = self.ids + spider.errors_ids;
        self.data = self.data + spider.errors_data
        
        connection = pymysql.connect(
            "localhost", "root", "root", "scrapy", charset='utf8')
        cursor = connection.cursor()
        
        if self.data:
            stmt = "INSERT INTO links (domain,firstLabel,secondLabel,title,link,status,hasData,isExternal) VALUES (%s, %s,%s, %s,%s,%s, %s,%s)"
            cursor.executemany(stmt, self.data)

        if self.ids:
            sql = 'delete from tmp_links where id in (' + ','.join(
            map(str, self.ids)) + ')'
            cursor.execute(sql)
        
        connection.commit()

        # sql = "INSERT INTO unique_links (link,isExternal) select distinct(link) as link,1 from tmp_links where isExternal=1 and link not in(select link from unique_links)"
        # cursor.execute(sql)   

        connection.commit()
        cursor.close()
        connection.close()
        

class SQLStorePipeline(object):

    data = []

    def __init__(self):
        self.connection = pymysql.connect(
            "localhost", "root", "root", "scrapy", charset='utf8')
        self.cursor = self.connection.cursor()

    def process_item(self, item, spider):


        if item['links']:
            for row in item['links']:

                link = {}
                link['status'] = 200
                link['isExternal'] = 0
                link['hasData'] = 0

                link['link'] = row['link']

                if link['link'].startswith('http') and row['domain'] in link['link']:
                    link['link'] = link['link'].replace(row['domain'],"")
                elif link['link'].startswith('http') and row['domain'].replace('www.',"") in link['link']:    
                    link['link'] = link['link'].replace(row['domain'].replace('www.',""),"")

                if link['link'].startswith('http'):
                    link['isExternal'] = 1
                elif not link['link'].startswith('/site/'):
                    link['status'] = 404

                
                if link['isExternal'] and not validators.url(link['link']):
                   link['status'] = 404
                elif not link['isExternal'] and not validators.url(row['domain']+link['link']):
                    link['status'] = 404   

                self.data.append((
                    row['domain'],
                    row['firstLabel'],
                    row['secondLabel'],
                    row['title'],
                    link['link'],
                    link['status'],
                    link['hasData'],
                    link['isExternal']
                ))

       
        return item

    def close_spider(self, spider):
        
        stmt = "INSERT INTO tmp_links (domain, firstLabel,secondLabel,title,link,status,hasData,isExternal) VALUES (%s, %s,%s, %s,%s,%s,%s,%s)"
        self.cursor.executemany(stmt, self.data)
        self.connection.commit()
        
        sql = "INSERT INTO unique_links (link,isExternal) select distinct(domain) as link,1 from tmp_links where domain not in(select link from unique_links)"
        self.cursor.execute(sql)


        sql = 'insert into links (domain,firstLabel,secondLabel,title,link,status,hasData,isExternal) select domain,firstLabel,secondLabel,title,link,status,hasData,isExternal from tmp_links where status=404 or firstLabel="সরকারি অফিস"'
        self.cursor.execute(sql)

        sql = 'delete from tmp_links where status=404 or firstLabel="সরকারি অফিস"'
        self.cursor.execute(sql)

        
        self.connection.commit()
        self.cursor.close()
        self.connection.close()