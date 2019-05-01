import datetime
import time
import csv
import re
import scrapy
import hashlib
import requests
import traceback
from random import randint
from lxml.html import fromstring
from scrapy.crawler import CrawlerProcess


PROXY = '125.27.10.209:59790'


class ExtractItem(scrapy.Item):
    name = scrapy.Field()
    short_description = scrapy.Field()
    detail = scrapy.Field()
    tags = scrapy.Field()
    established_date = scrapy.Field()
    size = scrapy.Field()
    address = scrapy.Field()
    branches = scrapy.Field()
    impressum_link = scrapy.Field()
    website = scrapy.Field()


class DasaugeSpider(scrapy.Spider):
    name = "dasauge_spider"
    allowed_domains = ["dasauge.de"]
    headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/72.0.3626.119 Safari/537.36',
    }

    def start_requests(self,):
        url = "https://dasauge.de/profile/agenturen/s1?KID%5B%5D=20&"\
               "KID%5B%5D=4&sort=edat&mitrel=0&mitarbeiter=5"
        yield scrapy.Request(
            url=url,
            callback=self.parse,
            headers=self.headers
        )

    def parse(self, response):
        results = response.xpath('//div[@class="neutral klickbatzen"]/h2/a')
        for result in results:
            item_url = result.xpath('@href').extract_first()
            if item_url:
                item_url = "https://dasauge.de" + item_url
                yield scrapy.Request(
                    url=item_url,
                    callback=self.parse_item,
                    headers=self.headers
                )
        next_page_url = response.xpath(
            '//a[@rel="next" or @rel="next nofollow"]/@href').extract_first()
        print('next_page_url')
        print(next_page_url)
        if next_page_url:
            next_page_url = "https://dasauge.de" + next_page_url
            yield scrapy.Request(
                url=next_page_url,
                callback=self.parse,
                headers=self.headers
            )

    def parse_item(self, response):
        item = ExtractItem()
        name = response.xpath(
            '//div[@itemprop="legalName brand"]/text()').extract_first()
        item['name'] = name.strip()

        short_description = response.xpath(
            '//div[@itemprop="legalName brand"]/'
            'following-sibling::span[1]/text()').extract_first()
        item['short_description'] = short_description.strip()

        detail_block = response.xpath(
            '//div[@itemprop="description"]')
        detail = detail_block.xpath('string()').extract()
        item['detail'] = '\n'.join(detail)

        tags = response.xpath('//ul[@class="tags"]//a/text()').extract()
        item['tags'] = ', '.join(tags)

        established_date = response.xpath(
            '//span[@itemprop="foundingDate"]/text()').extract_first()
        item['established_date'] = established_date

        size = response.xpath(
            '//li[em[text()="Mitarbeiter"]]/text()').extract()
        item['size'] = size[-1].strip() if size else None

        address_block = response.xpath('//td[@itemprop="address"]')
        address = address_block.xpath('string()').extract()
        item['address'] = '\n'.join([
            i.replace('\n', '').replace('\t', '').strip() for i in address
        ])

        branches = response.xpath(
            '//em[text()="Branchen"]/'
            'following-sibling::ul[1]/li/a/text()').extract()
        item['branches'] = '\n'.join([
            i.replace('\n', '').replace('\t', '').strip() for i in branches
        ])

        impressum_link = response.xpath(
            '//a[contains(text(), "Impressum")]/@href').extract_first()
        item['impressum_link'] = impressum_link

        website = response.xpath(
            '//td[text()="Web"]/'
            'following-sibling::td[1]/a/@href').extract_first()
        item['website'] = website
        yield item


def run_spider(no_of_threads, request_delay):
    settings = {
        "DOWNLOADER_MIDDLEWARES": {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'scrapy_fake_useragent.middleware.RandomUserAgentMiddleware': 400,
            'scrapy.downloadermiddlewares.retry.RetryMiddleware': 90,
            'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
            'rotating_proxies.middlewares.BanDetectionMiddleware': 620,
        },
        'ITEM_PIPELINES': {
            'pipelines.ExtractPipeline': 300,
        },
        'DOWNLOAD_DELAY': request_delay,
        'CONCURRENT_REQUESTS': no_of_threads,
        'CONCURRENT_REQUESTS_PER_DOMAIN': no_of_threads,
        'RETRY_HTTP_CODES': [403, 429, 500, 503],
        'ROTATING_PROXY_LIST': PROXY,
        'ROTATING_PROXY_BAN_POLICY': 'pipelines.BanPolicy',
        'RETRY_TIMES': 10,
        'LOG_ENABLED': True,

    }
    process = CrawlerProcess({
        'ITEM_PIPELINES': {
            'pipelines.ExtractPipeline': 300,
        },
        'DOWNLOAD_DELAY': request_delay,
        'CONCURRENT_REQUESTS': no_of_threads,
        'CONCURRENT_REQUESTS_PER_DOMAIN': no_of_threads,
    })
    process.crawl(DasaugeSpider)
    process.start()

if __name__ == '__main__':
    no_of_threads = 40
    request_delay = 0.1
    run_spider(no_of_threads, request_delay)
