import scrapy
from npf.items import NpfItem, DomainItem
import pymysql
import logging


class NpfSpider(scrapy.Spider):
    name = "npf"
    with open("my_urls.txt", "rt") as f:
        start_urls = ['http://' + url.strip() for url in f.readlines()]

    def closed(self, reason):
        print('Finised!')

    def removeWhiteSpace(self, word):
        if word is None:
            return ''
        return word.strip().strip("\'").strip('\"').replace("'", '').replace('"', '').replace(',', '')

    # def start_requests(self):
    #     db = pymysql.connect("localhost", "root", "root", "scrapy")
    #     cursor = db.cursor(pymysql.cursors.DictCursor)
    #     sql = "SELECT domain FROM `domains` where concat('http://',domain) not in (select distinct(domain) from tmp_links) and id=20128"
    #     cursor.execute(sql)
    #     result = cursor.fetchall()
    #     for row in result:
    #         url = 'http://' + self.removeWhiteSpace(row['domain'])
    #         yield scrapy.Request(url.strip(), self.parse)

    def parse(self, response):

        if response.css('#dawgdrops>ul>li') or response.css('div.box') or response.css('#right-content>div.right-block'):
            items = []
            self.logger.info('Parsing: %s', response.url)

            # Banner
            for slide in response.css('.rslides>li'):
                item = NpfItem()
                item['domain'] = response.url
                item['firstLabel'] = 'Main'
                item['secondLabel'] = 'Banner'
                item['title'] = self.removeWhiteSpace(slide.css(
                    "a::attr(title)").extract_first())
                item['link'] = self.removeWhiteSpace(slide.css(
                    "a::attr(href)").extract_first())

                items.append(item)

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

            domain = DomainItem()
            domain['name'] = response.url
            domain['links'] = items
            yield domain
        else:
            return
