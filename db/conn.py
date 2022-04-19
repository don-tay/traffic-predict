import os
import sys
import dotenv
import psycopg2

dotenv.load_dotenv()
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASS = os.environ["DB_PASS"]
DB_HOST = os.environ["DB_HOST"]
DB_PORT = os.environ["DB_PORT"]


def create_db_conn():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port=DB_PORT,
        )
        # set conn to autocommit queries after exec
        conn.autocommit = True
        return conn
    except Exception as e:
        print(e, file=sys.stderr)
        sys.exit(1)
