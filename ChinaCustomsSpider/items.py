# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class ChinacustomsspiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    weburl = scrapy.Field()
    title = scrapy.Field()
    keyword = scrapy.Field()
    dataupdate = scrapy.Field()
    contents = scrapy.Field()
    description = scrapy.Field()
    alltypes = scrapy.Field()
    pass
