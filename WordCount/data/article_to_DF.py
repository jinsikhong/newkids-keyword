import csv
import chardet
import pandas as pd

def get_article(article_path):
    # 기사 샘플 데이터 가져오기

    csv_file_path = article_path

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

    title = [row[0] for row in article_list[1:]]
    sub_title = [row[1] for row in article_list[1:]]
    writer = [row[2].strip('\n').strip() for row in article_list[1:]]
    published_date = [row[3].strip() for row in article_list[1:]]
    thumbnail_img = [row[4] for row in article_list[1:]]
    content = [row[5] for row in article_list[1:]]
    html_content = [row[6] for row in article_list[1:]]
    imgs = [row[7] for row in article_list[1:]]



    # print(first_column_data)
    article_df = pd.DataFrame(
        {"title" : title,
         "sub_title" : sub_title,
         "writer" : writer,
         "published_date" : published_date,
         "thumbnail_img" : thumbnail_img,
         "content" : content,
         "html_content" : html_content,
         "img" : imgs
         })

    return article_df


