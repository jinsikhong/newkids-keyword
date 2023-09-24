# 키워드 추출
from sklearn.feature_extraction.text import TfidfVectorizer

import csv
import chardet
# Initiate findspark
from konlpy.tag import Okt  # 형태소 분석기
import time
import pandas as pd
import os

os.environ["JAVA_HOME"] = "C:\Program Files\Java\jdk-11"
os.environ["SPARK_HOME"] = 'C:\Program Files\spark-3.4.1-bin-hadoop3/'
os.environ["HADOOP_HOME"] = 'C:\Program Files\spark-3.4.1-bin-hadoop3/'


def get_stopwords(csv_file_path):
    # 데이터 가져오기
    # 불용어 리스트 가져오기

    # csv_file_path = 'C:\SSAFY\Study\WordCount\불용어.csv'

    # 파일 인코딩을 감지하여 설정
    rawdata = open(csv_file_path, 'rb').read()
    result = chardet.detect(rawdata)
    encoding = result['encoding']

    # 불용어를 담을 list
    stop_word_list = []

    with open(csv_file_path, 'r', encoding=encoding) as csv_file:
        reader = csv.reader(csv_file)
        # 첫번째 행만 읽기
        first_row = next(reader)
        stop_word_list = first_row
    return set(stop_word_list)


def get_article(csv_file_path):
    # 기사 샘플 데이터 가져오기

    # csv_file_path = "C:/SSAFY/Study/WordCount/article_test_data.csv"

    # 파일 인코딩을 감지하여 설정
    rawdata = open(csv_file_path, 'rb').read()
    result = chardet.detect(rawdata)
    encoding = result['encoding']

    article_list = []
    with open(csv_file_path, 'r', encoding=encoding) as csv_file:
        reader = csv.reader(csv_file)
        # 첫번째 행만 읽기
        for line in reader:
            article_list.append(line)

    return article_list


def get_keyword(article_lst, stop_word_set):
    # 기사 데이터에서 키워드 추출
    # KoNLPy 형태소 분석기 초기화
    # Spark 활용 필터 적용

    # spark = SparkSession.builder.appName("TF-IDF Example").getOrCreate()

    keyword_list = []
    title_keyword_list = []

    okt = Okt()
    # 0          1           2           3                  4              5            6            7
    # ['title', 'sub_title', 'writer', 'published_date', 'thumbnail_img', 'content', 'html_content', 'imgs']


    for i in range(len(article_lst)):
        # 본문에서 뽑기
        res = okt.nouns(article_lst[i][5])
        # 제목에서 뽑기
        res.extend(okt.nouns(article_lst[i][1]))

        # No Rdd
        filtered_list = [word for word in res if word not in stop_word_set]

        keyword_list.append(filtered_list)

        # Use Rdd
        # start = time.time()
        # rdd = spark.sparkContext.parallelize(res)
        # end = time.time()
        # print(f"Paralleize 시간 : {end - start:.5f} sec")
        #
        # start = time.time()
        # rdd = rdd.filter(lambda word: word not in stop_word_set)
        # end = time.time()
        # print(f"filter 시간 : {end - start:.5f} sec")
        #
        # start = time.time()
        # keyword_list.append(rdd.collect())
        # end = time.time()
        # print(f"RDD collect 시간  : {end - start:.5f} sec")

    return keyword_list


def sort_coo(coo_matrix):
    """Sort a dict with highest score"""
    tuples = zip(coo_matrix.col, coo_matrix.data)
    return sorted(tuples, key=lambda x: (x[1], x[0]), reverse=True)


def extract_topn_from_vector(feature_names, sorted_items, topn=10):
    """get the feature names and tf-idf score of top n items"""

    # use only topn items from vector
    sorted_items = sorted_items[:topn]

    score_vals = []
    feature_vals = []

    # word index and corresponding tf-idf score
    for idx, score in sorted_items:
        # keep track of feature name and its corresponding score
        score_vals.append(round(score, 3))
        feature_vals.append(feature_names[idx])

    # create a tuples of feature, score
    results = {}
    for idx in range(len(feature_vals)):
        results[feature_vals[idx]] = score_vals[idx]

    return results


def get_top10_keywords(vectorizer, feature_names, doc):
    """Return top k keywords from a doc using TF-IDF method"""

    # generate tf-idf for the given document
    tf_idf_vector = vectorizer.transform([doc])

    # sort the tf-idf vectors by descending order of scores
    sorted_items = sort_coo(tf_idf_vector.tocoo())

    # extract only TOP_K_KEYWORDS
    keywords = extract_topn_from_vector(feature_names, sorted_items, 10)

    return ' '.join(list(keywords.keys()))


def calf_TFIDF(corpora):
    start = time.time()
    # Initializing TF-IDF Vectorizer with stopwords
    vectorizer = TfidfVectorizer(smooth_idf=True, use_idf=True)

    # Creating vocab with our corpora
    # Exlcluding first 10 docs for testing purpose

    # TF-IDF 벡터 행렬 -> 추천 알고리즘에 사용됨
    matrix = vectorizer.fit_transform(corpora)

    # Storing vocab
    feature_names = vectorizer.get_feature_names_out()
    end = time.time()

    print(f"TF-IDF 계산 시간 : {end - start:.5f} sec")
    return vectorizer, feature_names, matrix


def get_result(vectorizer, feature_names, corpora):
    result = []
    for doc in corpora:
        df = {}
        df['all_keywords'] = doc
        df['top_keywords'] = get_top10_keywords(vectorizer, feature_names, doc)
        result.append(df)

    final = pd.DataFrame(result)

    return final


# stop_word_set = get_stopwords()
# print("stop")
# article_list = get_article()
# print(article_list)
# start = time.time()
# keyword_list = get_keyword(article_list, stop_word_set)
# end = time.time()
# print(f"총 걸린 시간 : {end - start:.5f} sec")
#
# print(len(article_list))
# print(len(keyword_list))
#
# # tf-idf 계산
# corpora = [' '.join(keyword) for keyword in keyword_list]
#
# # 4. TF-IDF 계산
#
# start = time.time()
# # Initializing TF-IDF Vectorizer with stopwords
# vectorizer = TfidfVectorizer(smooth_idf=True, use_idf=True)
#
# # Creating vocab with our corpora
# # Exlcluding first 10 docs for testing purpose
#
# # TF-IDF 벡터 행렬 -> 추천 알고리즘에 사용됨
# matrix = vectorizer.fit_transform(corpora)
#
# # Storing vocab
# feature_names = vectorizer.get_feature_names_out()
# end = time.time()
#
# print(f"TF-IDF 계산 시간 : {end - start:.5f} sec")
#
# # 5. TOP10 핵심 키워드 추출
# # RESULT
# result = []
# for doc in corpora[0:30]:
#     df = {}
#     df['content'] = doc
#     df['top_keywords'] = get_keywords(vectorizer, feature_names, doc)
#     result.append(df)
#
# final = pd.DataFrame(result)
#
# article_df = pd.DataFrame({"title"})
#
# print(final)
