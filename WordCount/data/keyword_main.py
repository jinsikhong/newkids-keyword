import getKeyword as kw
# import db_connect
import article_to_DF as atd
import db_connect as db

import time
import os




# 환경변수 설정 (window)
os.environ["JAVA_HOME"] = "C:\Program Files\Java\jdk-11"
os.environ["SPARK_HOME"] = 'C:\Program Files\spark-3.4.1-bin-hadoop3/'
os.environ["HADOOP_HOME"] = 'C:\Program Files\spark-3.4.1-bin-hadoop3/'

stop_word_path = '..\불용어.csv'
# article_path = 'C:/SSAFY/Study/WordCount/crawl-data/combined_data.csv'
# article_path = '../crawl-data/combined_data.csv'
article_path = '../article_test_data.csv'

TOP_K_KEYWORDS = 10 # top k number of keywords to retrieve in a ranked document
# 불용어 리스트
stop_word_list = kw.get_stopwords(stop_word_path)
# 기사 dataframe
article_df = atd.get_article(article_path)
# 기사 리스트
article_list = article_df.values.tolist()
start = time.time()
print("키워드 추출 시작")
# 기사 별 키워드 리스트
keyword_list = kw.get_keyword(article_list, stop_word_list)
# 키워드를 문장으로 합치기
corpora = [' '.join(keyword) for keyword in keyword_list]
# 벡터라이저, 특징 이름(단어이름), tf-idf 벡터 행렬
vectorizer, feature_names, matrix = kw.calf_TFIDF(corpora)
print("tf-idf 행렬 벡터")
print(matrix)
# 키워드, 핵심 키워드를 담은 DF
result = kw.get_result(vectorizer, feature_names, corpora)
end = time.time()

article_df = article_df.join(result['all_keywords'])
article_df = article_df.join(result['top_keywords'])
print("title : " ,article_df['title'])
print("키워드 : ", article_df['all_keywords'])
print("탑 10 키워드 : ", article_df['top_keywords'])

print(f"키워드 추출 + TF-IDF 계산 시간 : {end - start:.5f} sec")
column_names = article_df.columns.tolist()

# 결과 출력
print(column_names)

db.insert_article(article_df)