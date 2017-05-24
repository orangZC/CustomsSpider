# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql
import sys

class ChinacustomsspiderPipeline(object):
    def __init__(self):
        # load(sys)
        self.conn = pymysql.connect(host="127.0.0.1", user="orange", password="@orangeLIU3226677zc", port=3306, db='CustomsData')
        self.conn.set_charset("utf8")

    def process_item(self, item, spider):
        title = item["title"]
        keyword = item['keyword']
        contents = item['contents']
        dataupdate = item['dataupdate']
        weburl = item['weburl']

        print(item["title"])
        print(item['keyword'])
        print(item['contents'])
        print(item['dataupdate'])
        print(item['weburl'])
        # return item
        sql = "insert into Customs(weburl, keyword, title ,dataupdate, contents) values(%s,%s,%s,%s,%s)"
        params = (
            weburl, keyword, title, dataupdate, contents
        )

        cursor = self.conn.cursor()
        try:
            cursor.execute(sql, params)
            self.conn.commit()
        except Exception as e:
            print(e)
            self.conn.rollback()
        # pass

    def close_spider(self):
        self.conn.close()
