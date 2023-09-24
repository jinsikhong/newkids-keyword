# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class NewsItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    
    title = scrapy.Field()
    sub_title = scrapy.Field()
    writer = scrapy.Field()
    published_date = scrapy.Field()
    thumbnail_img = scrapy.Field()
    content = scrapy.Field()
    imgs = scrapy.Field()
    html_content = scrapy.Field()
    pass
