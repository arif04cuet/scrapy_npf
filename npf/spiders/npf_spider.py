import scrapy
from npf.items import NpfItem, DomainItem


class NpfSpider(scrapy.Spider):
    name = "npf"
    #start_urls = ['http://www.comilla.gov.bd']

    def start_requests(self):
        for url in open('urls.txt'):
            yield scrapy.Request(url.strip(), self.parse)

    def parse(self, response):
        items = []
        self.logger.info('Parse function called on %s', response.url)
        # Main Manu
        for firstLabel in response.css('#dawgdrops>ul>li'):
            for secondLabel in firstLabel.css('div>div'):
                for thirdLabel in secondLabel.css('ul>li'):
                    item = NpfItem()
                    item['domain'] = response.url
                    item['firstLabel'] = firstLabel.css(
                        'a::attr(title)').extract_first()
                    item['secondLabel'] = secondLabel.css(
                        "h6::text").extract_first()
                    item['title'] = thirdLabel.css(
                        "a::attr(title)").extract_first()
                    item['link'] = thirdLabel.css(
                        "a::attr(href)").extract_first()
                    # yield item
                    items.append(item)

        # Service Box
        for box in response.css('div.box'):
            for link in box.css('ul>li'):
                item = NpfItem()
                item['domain'] = response.url
                item['firstLabel'] = 'Service Box'
                item['secondLabel'] = box.css("h4::text").extract_first()
                item['title'] = link.css("a::text").extract_first()
                item['link'] = link.css("a::attr(href)").extract_first()
                # yield item
                items.append(item)

        # Right Bar
        for box in response.css('#right-content>div.right-block'):
            ulli = box.css('ul>li')
            if ulli:
                for link in box.css('ul>li'):
                    item = NpfItem()
                    item['domain'] = response.url
                    item['firstLabel'] = 'Right Bar'
                    item['secondLabel'] = box.css("h5::text").extract_first()
                    item['title'] = link.css("a::text").extract_first()
                    item['link'] = link.css("a::attr(href)").extract_first()
                    # yield item
                    items.append(item)

            else:
                for link in box.css('a.share-buttons'):
                    item = NpfItem()
                    item['domain'] = response.url
                    item['firstLabel'] = 'Right Bar'
                    item['secondLabel'] = 'Social Media'
                    item['title'] = link.css("img::attr(alt)").extract_first()
                    item['link'] = link.css("::attr(href)").extract_first()
                    # yield item
                    items.append(item)

        domain = DomainItem()
        domain['name'] = items[0]['domain']
        domain['links'] = items
        yield domain
