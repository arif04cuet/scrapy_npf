import scrapy
from npf.items import NpfItem,TmpItem
import pymysql
import logging
import time
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError


class Npf2Spider(scrapy.Spider):
    name = "npf2"
    custom_settings = {
            'ITEM_PIPELINES': {
                'npf.pipelines.Npf2Pipeline': 1
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

        item = failure.request.meta['row']
        self.errors_data.append(
            (
                item['domain'],
                item['firstLabel'],
                item['secondLabel'],
                item['title'],
                item['link'],
                404,
                0,
                item['isExternal']
            )
        )
        self.errors_ids.append(item['id'])

    def start_requests(self):
        db = pymysql.connect("localhost", "root", "root", "scrapy",charset='utf8')
        cursor = db.cursor(pymysql.cursors.DictCursor)
        sql = "select id,domain,firstLabel,secondLabel,title,link,status,hasData,isExternal from tmp_links where isExternal=0 order by domain"
        cursor.execute(sql)
        result = cursor.fetchall()
        for row in result:
            url = row['domain']+'/'+row['link']
            if row['isExternal']:
                url = row['link']
            
            request = scrapy.Request(url.strip(),method='GET',callback=self.parse,errback=self.errback_parse)
            request.meta['row'] = row
            #request.meta['proxy'] = '127.0.0.1'
            yield request

    def parse(self, response):
        
        content = response.css('div#printable_area').extract_first()
        row = response.meta['row']

        if content is None:
            row['status'] = 404
            row['hasData'] = 0
        else:
            length = len(response.css('div#printable_area').extract_first())
            row['status'] = response.status
            row['hasData'] = length

        yield row
