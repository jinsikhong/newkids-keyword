import pandas as pd
import chardet

# CSV 파일 경로
csv_file_paths = [
    "../crawl-data/동아일보.csv",
    "../crawl-data/어린이경제신문1.csv",
    "../crawl-data/어린이경제신문2.csv",
    "../crawl-data/어린이조선일보.csv",
    "../crawl-data/중앙일보.csv"
]

# 빈 DataFrame 생성
combined_df = pd.DataFrame()

# CSV 파일을 순회하며 합치기
for file_path in csv_file_paths:
    # 파일 인코딩을 감지하여 설정
    print(file_path)
    rawdata = open(file_path, 'rb').read()
    result = chardet.detect(rawdata)
    encoding = result['encoding']

    # CSV 파일을 읽어와 DataFrame으로 변환
    df = pd.read_csv(file_path, encoding=encoding)

    # 읽어온 DataFrame을 기존에 만든 DataFrame에 추가
    combined_df = pd.concat([combined_df, df], ignore_index=True)

# 하나로 합쳐진 DataFrame을 CSV 파일로 저장 (선택 사항)
combined_df.to_csv("../crawl-data/combined_data.csv",  encoding=encoding, index=False)

# 결과 출력 (합쳐진 DataFrame)
print(combined_df)
