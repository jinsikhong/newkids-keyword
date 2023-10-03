from sqlalchemy import create_engine
import pymysql
import pandas as pd
def get_crawl_article():
    # global conn
    try:
        # connection = pymysql.connect(
        #     host="127.0.0.1",
        #     port=3306,
        #     user='root',
        #     password='12341234',
        #     db='newkids_test',
        #     charset='utf8'
        #
        # )
        # # 커서 생성
        # cursor = connection.cursor()
        #
        # # article 테이블에서 전체 데이터 가져오기
        # query = "SELECT * FROM article"
        # cursor.execute(query)
        #
        # # 결과 가져오기
        # rows = cursor.fetchall()
        #
        # df = pd.DataFrame(rows, columns=[desc[0] for desc in cursor.description])
        #
        # return df
        #
        #   방법 2
        db_connection_path = 'mysql+pymysql://root:12341234@127.0.0.1:3306/newkids_test'
        db_connection = create_engine(db_connection_path)
        connection = db_connection.connect()
        sql = (
            "SELECT published_date FROM article WHERE article_id = 252"
        )

        result = pd.read_sql_query(sql, connection)
        print(type(result['published_date'][0]))
        print(12312312)
        return result

    except Exception as e:
        print(f"Exception={e}")
    finally:
        if connection:
            connection.close()
print(type(get_crawl_article()['published_date']))