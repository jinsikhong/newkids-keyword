# csv 파일 로드
dir_path = "C://Users/SSAFY/Desktop/article_data/articles"
dirs = os.listdir(dir_path)
print(dirs)
# csv 파일 병함
articles_df = pd.DataFrame()
for file in dirs:
    df = pd.read_csv(os.path.join(dir_path, file), encoding="ansi").loc[:, ['제목', '키워드', '본문']]
    # print(df.shape)
    articles_df = pd.concat([articles_df, df])
    # break

# 인덱스 새로 부여
# print(articles_df.index)
# 샘플 사이즈 조절
# sample_size = 30000
# articles_df = (articles_df.iloc)[:sample_size]
print(articles_df.shape)

# 키워드를 공백으로 분할 (, 로 분할되어있는 문장)
articles_df['키워드'] = articles_df['키워드'].apply(lambda x: x.split(", "))

# TF-IDF 벡터화
start = time.time()
tfidf_vectorizer = TfidfVectorizer()
tfidf_matrix = tfidf_vectorizer.fit_transform(
    [' '.join(keywords) for keywords in articles_df['키워드']])  # i 번 기사에 대한 각 keyword_vector 에 대한 가중치가 결과로 나옴
end = time.time()
print(f"TF-IDF Vectorize time: {end - start} sec")
print("TF-IDF Vectorize done")
print(f"TF-IDF Vector shape: {tfidf_matrix.shape}")
print(tfidf_matrix[:5])