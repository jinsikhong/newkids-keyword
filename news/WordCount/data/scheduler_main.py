# import getKeyword as kw
import WordCount.data.getKeyword as kw
# import article_to_DF as atd
import WordCount.data.db_connect as db
# import db_connect as db
import time
# 크롤링하고 키워드 추출 후 DB에 insert하는 스크립트(스케줄러 사용)
# 스케줄링 돌아가는 main 스크립트
# 동작 방식
# 1. 4개의 신문사에서 데이터 크롤링
# 2. 데이터에서 키워드 추출
# 3. newkids article 테이블에 넣기

def get_keyword_by_crawl_data(items):
    # 크롤링을 통해 얻은 기사 DF
    crawl_data = items
    # 변수 선언
    # 불용어 리스트

    stop_word_path = ('../WordCount/불용어.csv')
    # stop_word_path = ('../불용어.csv')
    # top k number of keywords to retrieve in a ranked document
    TOP_K_KEYWORDS = 10

    #불용어 리스트
    stop_word_list = kw.get_stopwords(stop_word_path)
    #크롤링한 기사 목록을 저장한 df
    article_df = crawl_data
    # df를 리스트로 변환
    article_list = article_df.values.tolist()

    print("키워드 추출 시작")
    start = time.time()
    #기사 별 추출한 키워드 리스트
    keyword_list = kw.get_keyword(article_list, stop_word_list)
    end = time.time()
    print("키워드 리스트")
    print(keyword_list)
    print(f"키워드 추출 : {end - start:.5f} sec")

    # 카워드를 하나의 String으로 합치기
    corpora = [' '.join(keyword) for keyword in keyword_list]
    # 벡터라이저, 특징 이름(단어이름), tf-idf 벡터 행렬
    vectorizer, feature_names, matrix = kw.calf_TFIDF(corpora)
    # 키워드, 핵심 키워드르, 기사 정보를 담은 DF
    result = kw.get_result(vectorizer, feature_names, corpora)
    # 결과 합치기
    article_df['all_keywords'] = result['all_keywords']
    article_df['top_keywords'] = result['top_keywords']

    print("키워드 : ", article_df['all_keywords'])
    print("탑 10 키워드 : ", article_df['top_keywords'])

    start = time.time()
    # 데이터베이스에 insert
    db.insert_article(article_df)
    end = time.time()
    print(f"db insert 시간 : {end - start:.5f} sec")


# get_keyword_by_crawl_data(123)