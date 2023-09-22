import os
import time

import pandas as pd
from sqlalchemy import create_engine
import pymysql
from sklearn.feature_extraction.text import TfidfVectorizer





# article data형식에 맞는 tupel 리턴
def insert_article_make_data(article_df):
    title = article_df['title'].tolist()
    sub_title = article_df['sub_title'].tolist()
    writer = article_df['writer'].tolist()
    published_date = article_df['published_date'].tolist()
    content = article_df['content'].tolist()
    html_content = article_df['html_content'].tolist()
    all_keywords = article_df['all_keywords'].tolist()
    print("타이틀 리스트 : ", title[0])

    input_data = list(zip(title, sub_title, writer, published_date, content, html_content, all_keywords))
    print(input_data[0])
    return input_data



def select_article():
    return None








def insert_article(article_df):
    db_connection_path = 'mysql+pymysql://root:root@127.0.0.1:3306/newkids'
    db_connection = create_engine(db_connection_path)
    conn = db_connection.connect()

    df = article_df[['title','sub_title','writer','published_date', 'content', 'thumbnail_img','all_keywords', 'top_keywords', 'html_content']]

    df.to_sql(name='article', con = db_connection, if_exists='append', index=False)
    # columns = ['title', 'sub_title', 'writer', 'published_date', 'content', 'thumbnail_img', 'all_keywords', 'html_content'])
    conn.close()

    # connection = None
    # insert_data = insert_article_make_data(article_df)
    # try:
    #     connection = my.connect(
    #         host="127.0.0.1",
    #         port=3306,
    #         user='root',
    #         password='root',
    #         db='newkids',
    #         charset='utf8'
    #     )
    #
    #     cursor = connection.cursor()
    #
    #     sql = (
    #         "INSERT INTO `article` (title, sub_title, writer, published_date, content, html_contnet, all_keywords) "
    #         "VALUES (%s, %s, %s, %s, %s, %s. %s) "
    #     )
    #
    #     cursor.executemany(sql, insert_data)
    #     connection.commit()
    # except Exception as e:
    #     print(f"Exception={e}")
    # finally:
    #     if connection:
    #         connection.close()

# ---------------------------------done -------------------




#
# dir_path = "C://Users/SSAFY/Desktop/article_data/articles"
# dirs = os.listdir(dir_path)
# print(dirs)
# # csv 파일 병합 -> 첫번째 파일만 사용
# articles_df = pd.DataFrame()
# for file in dirs:
#     df = pd.read_csv(os.path.join(dir_path, file), encoding="ansi").loc[:, ['제목', '키워드', '본문']]
#     articles_df = pd.concat([articles_df, df])
#     break
#   `
# # nan 데이터 제거 및 인덱스 재배치
# articles_df = articles_df.dropna()
# articles_df.index = range(1, len(articles_df) + 1)
# print(articles_df.shape)
# print(articles_df.head(1))
#
# dict = articles_df[['제목', '키워드']].to_dict()
# titles = list(dict['제목'].values())
# all_keywords = list(dict['키워드'].values())
# print(titles[0])
# print(all_keywords[0])
#
#
# insert_article(list(zip(titles, all_keywords)))
#
# print("insert article process done.")
#
#
# # 키워드를 공백으로 분할 (, 로 분할되어있는 문장)
# keywords_df = articles_df['키워드'].apply(lambda x: x.split(", "))
# print(keywords_df.head(1))
#
# # TF-IDF 벡터화
# start = time.time()
# tfidf_vectorizer = TfidfVectorizer()
# tfidf_matrix = tfidf_vectorizer.fit_transform(
#     [' '.join(keywords) for keywords in keywords_df])  # i 번 기사에 대한 각 keyword_vector 에 대한 가중치가 결과로 나옴
# end = time.time()
# print(f"TF-IDF Vectorize time: {end - start} sec")
# print("TF-IDF Vectorize done")
# print(f"TF-IDF Vector shape: {tfidf_matrix.shape}")


def insert_tfidf(data_list):
    connection = None

    try:
        connection = pymysql.connect(
            host="127.0.0.1",
            port=3306,
            user='ssafy',
            password='ssafy',
            db='newkids',
            charset='utf8'
        )

        cursor = connection.cursor()

        sql = (
            "INSERT INTO `article_tfidf` (article_id, keyword_vector, weight) "
            "VALUES (%s, %s, %s) "
        )

        cursor.executemany(sql, data_list)
        connection.commit()
    except Exception as e:
        print(f"Exception={e}")
    finally:
        if connection:
            connection.close()

#
# data_list = [(row+1, col, value) for row, col, value in zip(*tfidf_matrix.nonzero(), tfidf_matrix.data)]
# print(data_list[0])
# insert_tfidf(data_list)


class NewsPipeline:
    def __init__(self):
        self.connect = pymysql.connect(
            host='localhost',
            db='test',
            user='root',
            password='ssafyc204',
        )
        self.cursor = self.connect.cursor()

    def process_item(self, item, spider):
        try:
            self.cursor.execute("SELECT * FROM article WHERE title = %s", (item['title'],))
            existing_data = self.cursor.fetchone()

            if not existing_data:
                self.cursor.execute(
                    # "INSERT INTO article (title, sub_title, writer, published_date, content, thumbnail_img, create_date, last_modified_date, all_keywords, html_content, url) value(%s, %s, %s, %s, %s, %s, now(), now(), '1', %s, %s)",
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

                # self.cursor.execute(
                #     "INSERT INTO article_image "
                #     "(url) "
                #     "VALUES "
                #     "(%s)",
                #     (
                #         item['imgs'],
                #     ))

                # self.connect.commit()
            else:
                print(f"제목 '{item['title']}'이 이미 존재합니다.")
        except Exception as e:
            print(e)

        return item

    def close_spider(self, spider):
        self.connect.close()