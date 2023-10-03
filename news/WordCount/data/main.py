import os
import time
import getKeyword as kw
# import db_connect
import article_to_DF as atd

import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity


# 코사인 유사도 추천 함수
def get_recommenations_by_cosine(idx, cosine_sim, data):
    # idx = data.index[article_no][0]
    sim_scores = list(enumerate(cosine_sim[idx]))
    sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)
    sim_scores = sim_scores[1:]  # 자기자신 제외
    articles_indices = [i[0] for i in sim_scores]
    return data.iloc[articles_indices]


# pd.set_option('display.max_rows', None)
# pd.set_option('display.max_columns', None)
# pd.set_option('display.max_colwidth', None)
#
# # csv 파일 로드
# dir_path = "C://Users/SSAFY/Desktop/article_data/articles"
# dirs = os.listdir(dir_path)
# print(dirs)
# # csv 파일 병함
# articles_df = pd.DataFrame()
# for file in dirs:
#     df = pd.read_csv(os.path.join(dir_path, file), encoding="ansi").loc[:, ['제목', '키워드', '본문']]
#     # print(df.shape)
#     articles_df = pd.concat([articles_df, df])
#     # break
#
# # 인덱스 새로 부여
# print(articles_df.index)
# # 샘플 사이즈 조절
# # sample_size = 1000
# # articles_df = articles_df.iloc[:1000]
# print(articles_df.shape)
#
# # 키워드를 공백으로 분할 (, 로 분할되어있는 문장)
# articles_df['키워드'] = articles_df['키워드'].apply(lambda x: x.split(", "))
#
stop_word_path = '..\불용어.csv'
# article_path = '../article_test_data.csv'
article_path = '../crawl-data/combined_data.csv'
stop_word_list = kw.get_stopwords(stop_word_path)
# 기사 dataframe
article_df = atd.get_article(article_path)
# 기사 리스트
article_list = article_df.values.tolist()
print("키워드 추출 시작")
# 기사 별 키워드 리스트
keyword_list = kw.get_keyword(article_list, stop_word_list)
# 키워드를 문장으로 합치기
corpora = [' '.join(keyword) for keyword in keyword_list]

# TF-IDF 벡터화

tfidf_vectorizer = TfidfVectorizer()
# 문서 단어 행렬

vectorizer, feature_names, matrix = kw.calf_TFIDF(corpora)
result = kw.get_result(vectorizer, feature_names, corpora)
article_df = article_df.join(result['all_keywords'])
article_df = article_df.join(result['top_keywords'])


start = time.time()
tfidf_matrix = tfidf_vectorizer.fit_transform(result['all_keywords'])  # i 번 기사에 대한 각 keyword_vector 에 대한 가중치가 결과로 나옴


end = time.time()
print(f"TF-IDF 벡터화 소요 시간: {end - start} sec")
print("TF-IDF 벡터화 종료")
print(tfidf_matrix)

# 코사인 유사도 계산 (일반)
start = time.time()
idx = 0
similarity = cosine_similarity(tfidf_matrix[idx], tfidf_matrix)
print("cosine similarity shape", similarity.shape)
print("cosine similarity done.")
end = time.time()
print(f"{end - start: .5f} sec")

# 추천 (일반)
recommended_articles = get_recommenations_by_cosine(idx=idx, cosine_sim=similarity, data=article_df)
# print(c.loc[0]['제목'])

print(result['top_keywords'][idx])
print(recommended_articles['title'][:10])
print(recommended_articles['all_keywords'][:10])

# ================================================================================================== #
# 상위 키워드 개수 설정
# top_keywords = 5
#
# # 각 기사별로 상위 키워드 인덱스를 추출합니다.
# top_keywords_indices = []
#
# for i in range(tfidf_matrix.shape[0]):
#     sorted_indices = np.argsort(-tfidf_matrix[i].toarray().flatten())
#     top_indices = sorted_indices[:top_keywords]
#     top_keywords_indices.append(top_indices)
#
# # 상위 키워드에 대한 가중치만을 가지는 행렬 생성
# tfidf_matrix_top_keywords = np.zeros(tfidf_matrix.shape)
#
# for i, top_indices in enumerate(top_keywords_indices):
#     tfidf_matrix_top_keywords[i, top_indices] = tfidf_matrix[i, top_indices].toarray()
#
# print(tfidf_matrix_top_keywords)

""" 
컨텐츠 기반 필터링
문제점
TF-IDF 벡터화 작업, 데이터 로드 작업 모두가 로컬에서 수행
-> 메모리 부족으로 에러 발생 가능성이 있음
=> 코사인 유사도 계산 후 추천만 수행할 수 있도록 입력을 tfidf_matrix 로.
"""
# start = time.time()
# # 코사인 유사도 계산 (가중치 상위 5개 키워드 사용)
# similarity = cosine_similarity(tfidf_matrix_top_keywords, tfidf_matrix_top_keywords)
# print("cosine similarity done.")
# end = time.time()
# print(f"{end - start: .5f} sec")
#
# # 기사에 대한 추천 확인
# start = time.time()
# recommended_articles = get_recommenations_by_cosine(idx=0, cosine_sim=similarity, data=articles_df)
# print(articles_df['제목'].head(1))
#
# print(recommended_articles['제목'][:10])
# end = time.time()
# print(f"{end - start: .5f} sec")

# ============================================================================================================== #
