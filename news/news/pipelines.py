# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pandas as pd
import pymysql
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))
 
from WordCount.data import scheduler_main as sc 

class NewsPipeline:
    def __init__(self):
        self.connect = pymysql.connect(
            host = 'localhost',
            db = 'test1',
            user = 'root',
            password = 'ssafyc204',
        )
        self.cursor = self.connect.cursor()

    def process_item(self, item, spider):
        try:
            self.cursor.execute("SELECT * FROM article WHERE title = %s and published_date = %s ", (item['title'], item['published_date']))
            existing_data = self.cursor.fetchone()

            if not existing_data:

                self.items = []
                self.items.append({
                        'title': item['title'],
                        'sub_title': item['sub_title'],
                        'writer': item['writer'],
                        'published_date': item['published_date'],
                        'content': item['content'],
                        'thumbnail_img': item['thumbnail_img'],
                        'html_content': item['html_content'],
                    })
                
                pandas_items = pd.DataFrame(self.items)
                print("#####################")
                print(type(pandas_items))
                print(pandas_items)
                print(pandas_items.columns.tolist())
                sc.get_keyword_by_crawl_data(pandas_items)

                self.cursor.execute(
                    "INSERT INTO article "
                    "(title, sub_title, writer, published_date, content, thumbnail_img, "
                    "all_keywords, html_content) "
                    "VALUES "
                    "(%s, %s, %s, %s, %s, %s, '1', %s)",
                    (
                        item['title'],
                        item['sub_title'],
                        item['writer'],
                        item['published_date'],
                        item['content'],
                        item['thumbnail_img'],
                        item['html_content'],
                    ))
                self.connect.commit()

            else:
                print(f"제목 '{item['title']}'이 이미 존재합니다.")
        except Exception as e:
            print(e)

        return item
    
    def close_spider(self, spider):
        self.connect.close()