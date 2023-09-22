from sqlalchemy import create_engine
import pymysql
import pandas as pd
def get_crawl_article():
    try:
        # connection = pymysql.connect(
        #     host="127.0.0.1",
        #     port=3306,
        #     user='root',
        #     password='root',
        #     db='newkids',
        #     charset='utf8'
        #
        # )
        #
        #
        db_connection_path = 'mysql+pymysql://root:root@127.0.0.1:3306/newkids'
        db_connection = create_engine(db_connection_path)
        conn = db_connection.connect()
        sql = (
            "SELECT * FROM article"
        )

        result = pd.read_sql_query(sql, conn)
        print(result['article_id'])
        return result

    except Exception as e:
        print(f"Exception={e}")
    finally:
        if conn:
            conn.close()

get_crawl_article()