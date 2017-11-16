import scrapy
from npf.items import NpfItem,TmpItem
import pymysql
import logging
import time
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError


class Npf3Spider(scrapy.Spider):
    name = "npf3"
    custom_settings = {
            'ITEM_PIPELINES': {
                'npf.pipelines.NpfExternalPipeline': 1
            }
        }
    start_time = time.time()
    errors_data = []
    errors_ids = []
    
    def removeWhiteSpace(self, word):
        if word is None:
            return ''
        return word.strip().strip("\'").strip('\"').replace("'", '').replace('"', '').replace(',', '')

    def closed(self, reason):
        end = time.time()
        print(end - self.start_time)

    def errback_parse(self, failure):

        url = failure.request.url
        self.errors_data.append(
            (
               
                url,
                404,
                1
            )
        )
        

    def start_requests(self):
        db = pymysql.connect("localhost", "root", "root", "scrapy",charset='utf8')
        cursor = db.cursor(pymysql.cursors.DictCursor)
        sql = "select distinct(link) as link from tmp_links where isExternal=1 and link not in( select link from unique_links)"
        cursor.execute(sql)
        result = cursor.fetchall()
        for row in result:
            url = row['link']
            request = scrapy.Request(url.strip(),method='HEAD',callback=self.parse,errback=self.errback_parse)
            request.meta['download_timeout'] = 40
            yield request

    def parse(self, response):
        
        row = {}
        row['link'] = response.url
        row['status'] = response.status
        
        yield row
