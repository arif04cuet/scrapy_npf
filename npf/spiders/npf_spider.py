import scrapy
from npf.items import NpfItem, DomainItem
import pymysql
import logging


class NpfSpider(scrapy.Spider):
    name = "npf"
    #start_urls = ['http://www.comilla.gov.bd']

    def removeWhiteSpace(self, word):
        if word is None:
            return ''
        return word.strip().strip("\'").strip('\"').replace("'", '').replace('"', '').replace(',', '')

    def start_requests(self):
        db = pymysql.connect("localhost", "root", "root", "scrapy")
        cursor = db.cursor()
        sql = "select domain from scraped_domains where crawled=0 and domain_type in ('Division','District','Upazilla') order by FIND_IN_SET(domain_type,'Division,District,Upazilla,Union')"
        cursor.execute(sql)
        result = cursor.fetchall()
        for row in result:
            url = 'http://' + self.removeWhiteSpace(row[0])

            yield scrapy.Request(url.strip(), self.parse)

    def parse(self, response):

        if response.css('#dawgdrops>ul>li') or response.css('div.box') or response.css('#right-content>div.right-block'):
            items = []
            self.logger.info('Parse function called on %s', response.url)
            # Main Manu
            for firstLabel in response.css('#dawgdrops>ul>li'):
                for secondLabel in firstLabel.css('div>div'):
                    for thirdLabel in secondLabel.css('ul>li'):
                        item = NpfItem()
                        item['domain'] = response.url
                        item['firstLabel'] = self.removeWhiteSpace(firstLabel.css(
                            'a::attr(title)').extract_first())
                        item['secondLabel'] = self.removeWhiteSpace(secondLabel.css(
                            "h6::text").extract_first())
                        item['title'] = self.removeWhiteSpace(thirdLabel.css(
                            "a::attr(title)").extract_first())
                        item['link'] = self.removeWhiteSpace(thirdLabel.css(
                            "a::attr(href)").extract_first())

                        items.append(item)

            # Service Box
            for box in response.css('div.box'):
                for link in box.css('ul>li'):
                    item = NpfItem()
                    item['domain'] = response.url
                    item['firstLabel'] = 'Service Box'
                    item['secondLabel'] = self.removeWhiteSpace(
                        box.css("h4::text").extract_first())
                    item['title'] = self.removeWhiteSpace(
                        link.css("a::text").extract_first())
                    item['link'] = self.removeWhiteSpace(
                        link.css("a::attr(href)").extract_first())
                    items.append(item)

            # Right Bar
            for box in response.css('#right-content>div.right-block'):

                ulli = box.css('ul>li')
                if ulli:
                    for link in box.css('ul>li'):
                        item = NpfItem()
                        item['domain'] = response.url
                        item['firstLabel'] = 'Right Bar'
                        item['secondLabel'] = self.removeWhiteSpace(box.css(
                            "h5::text").extract_first())
                        item['title'] = self.removeWhiteSpace(
                            link.css("a::text").extract_first())
                        item['link'] = self.removeWhiteSpace(link.css(
                            "a::attr(href)").extract_first())

                        if item['secondLabel'] != 'কেন্দ্রীয় ই-সেবা':
                            items.append(item)

                else:
                    for link in box.css('a.share-buttons'):
                        item = NpfItem()
                        item['domain'] = response.url
                        item['firstLabel'] = 'Right Bar'
                        item['secondLabel'] = 'Social Media'
                        item['title'] = self.removeWhiteSpace(link.css(
                            "img::attr(alt)").extract_first())
                        item['link'] = self.removeWhiteSpace(
                            link.css("::attr(href)").extract_first())
                        items.append(item)

            domain = DomainItem()
            domain['name'] = items[0]['domain']
            domain['links'] = items
            yield domain
        else:
            return
