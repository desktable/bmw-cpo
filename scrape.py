#!/usr/bin/env python

"""
TODO:
- Carfax date
- Adding cache
"""

import datetime
import time

from pandas import DataFrame
import requests
import scrapy
from scrapy.item import Field, Item
from scrapy.contrib.loader import ItemLoader
from scrapy.contrib.loader.processor import TakeFirst

URL_TEMPLATE = ('http://cpo.bmwusa.com/used-inventory/index.htm?'
                'superModel=X6&compositeType=used&geoZip=07310&start={}')

class Vehicle(Item):
    vin      = Field()
    price    = Field()
    carfax   = Field()
    url      = Field()
    title    = Field()
    year     = Field()
    miles    = Field()
    dealer   = Field()
    distance = Field()
    exterior = Field()
    interior = Field()

ret = []
start = 1
has_error = False
while not has_error:
    url = URL_TEMPLATE.format(start)
    print url
    r = requests.get(url)
    if r.status_code != 200:
        print r.status_code
        break
    sel = scrapy.Selector(text=r.text)
    for div_car in sel.xpath('//div[@data-classification="primary"]'):
        l = ItemLoader(Vehicle(), div_car)
        l.add_xpath('vin', '@data-vin')
        PRICING_BADGE_XPATH = './/div[@class="pricing-badges"]'
        l.add_xpath('price',  PRICING_BADGE_XPATH +
                    '//span[@class="value"]/text()')
        l.add_xpath('carfax', PRICING_BADGE_XPATH +
                    '//li[@class="carfax"]/a/@href')
        TITLE_XPATH = './/div[@class="title-description"]'
        l.add_xpath('url',    TITLE_XPATH + '//h1/a[@class="url"]/@href')
        l.add_xpath('title',  TITLE_XPATH + '//h1/a[@class="url"]/text()')
        l.add_xpath('year',  TITLE_XPATH + '//h1/a[@class="url"]/text()',
                    re=r'([0-9]{4})')
        l.add_xpath('miles',  TITLE_XPATH +
                    '//div[contains(@class,"bodystyle-odometer")]/strong/text()',
                    re=r'(.*) miles')
        l.add_xpath('distance', TITLE_XPATH +
                    '//div[contains(@class,"dealer-distance")]/p/strong/text()')
        l.add_xpath('dealer',   TITLE_XPATH +
                    '//div[contains(@class,"dealer-link")]/a/text()')
        l.add_xpath('exterior', TITLE_XPATH + '//ul/li/text()',
                    re=r'Exterior Color: (.*)')
        l.add_xpath('interior', TITLE_XPATH + '//ul/li/text()',
                    re=r'Interior: (.*)')
        item = {k: v[0].replace('\n', ' ') if v else None
                for k, v in l.load_item().iteritems()}
        if 'vin' in item:
            vin = item['vin']
            if vin.startswith('5UXFG2') or vin.startswith('5UXFG4') \
                    or vin.startswith('5UXKU2'):
                item['trim'] = 'xDrive35i'
            elif vin.startswith('5UXFG8') or vin.startswith('5UXKU6'):
                item['trim'] = 'xDrive50i'
            elif vin.startswith('5UXKU0'):
                item['trim'] = 'sDrive35i'
        if 'miles' in item:
            item['miles'] = int(item['miles'].strip().replace(',',''))
        if 'price' in item:
            item['price'] = int(item['price'].strip('$').replace(',',''))
        if 'url' in item:
            item['certified'] = 'cert' if 'certified' in item['url'] else 'used'
            item['url'] = 'http://cpo.bmwusa.com' + item['url']
        if 'title' in item:
            if 'X6' in item['title']:
                ret.append(item)
            else:
                # When we exhaust the items, we get results that are not related
                has_error = True
    start += 15
    time.sleep(1)

for item in ret:
    if 'url' in item:
        url = item['url']
        print url
        r = requests.get(url)
        if r.status_code != 200:
            print r.status_code
            break
        sel = scrapy.Selector(text=r.text)
        pkgs = sel.xpath('//span[@class="packageName"]/text()').extract()
        item['pkgs'] = ' / '.join(sorted(set(pkgs)))
        time.sleep(1)
    if 'carfax' in item:
        carfax = item['carfax']
        print carfax
        r = requests.get(carfax)
        if r.status_code != 200:
            print r.status_code
            break
        sel = scrapy.Selector(text=r.text)
        first_date = None
        last_sale  = None
        last_sold  = None
        for entry in sel.xpath('//tr[contains(@id, "record")]'):
            date = (entry.xpath('td/text()')
                         .re(r'([0-9]{2}/[0-9]{2}/[0-9]{4})'))
            if date:
                date = datetime.datetime.strptime(date[0], '%m/%d/%Y')
                if first_date is None:
                    first_date = date
                if entry.xpath('td/text()').re(r'for sale'):
                    last_sale = date
                if entry.xpath('td/text()').re(r'sold'):
                    last_sold = date
        if first_date:
            item['first_date'] = first_date.strftime('%Y/%m/%d')
        if last_sale:
            item['last_sale'] = last_sale.strftime('%Y/%m/%d')
            item['period'] = (datetime.datetime.now() - last_sale).days
            print item['period']
        if last_sold:
            item['last_sold'] = last_sold.strftime('%Y/%m/%d')
        if last_sale and last_sold and last_sold > last_sale:
            item['period'] = 'SOLD'
        time.sleep(1)

df = DataFrame(ret).drop_duplicates('vin').sort('price')
df[['year', 'trim', 'certified', 'miles', 'price', 'dealer', 'distance',
    'period', 'exterior', 'interior',
    'first_date', 'last_sale', 'last_sold',
    'pkgs', 'title', 'vin', 'url', 'carfax']].to_csv('data.csv')
