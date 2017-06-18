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
        self.conn = pymysql.connect(host="127.0.0.1",
                                    user="orange",
                                    password="@orangeLIU3226677zc",
                                    port=3306,
                                    db='customs_data',
                                    charset='utf8')
        self.conn.set_charset("utf8")

    def process_item(self, item, spider):
        title = item["title"]
        keyword = item['keyword']
        contents = item['contents']
        dataupdate = item['dataupdate']
        weburl = item['weburl']
        description = item['description']
        alltypes = item['alltypes']

        print(item["title"])
        print(item['keyword'])
        print(item['description'])
        print(item['contents'])
        print(item['dataupdate'])
        print(item['weburl'])
        print(item['alltypes'])
        # return item
        sql = "insert into customs_table1(weburl, keyword, title ,dataupdate, description, contents, alltypes) values(%s,%s,%s,%s,%s,%s,%s)"
        params = (
            weburl, keyword, title, dataupdate, description, contents, alltypes
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
