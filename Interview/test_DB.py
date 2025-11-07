# debug_db_connect.py
import os
from urllib.parse import quote_plus
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

load_dotenv(dotenv_path=r"C:\Users\sreed\OneDrive\Desktop\Interview\.env")

host = os.getenv("MYSQL_HOST","localhost")
port = os.getenv("MYSQL_PORT","3306")
user = os.getenv("MYSQL_USER","root")
pw = os.getenv("MYSQL_PASS","Sreedatta@123")
db = os.getenv("MYSQL_DB","divami_sales")

print("Trying to connect to:", host, port, user, db)
db_url = f"mysql+mysqlconnector://{user}:{quote_plus(pw)}@{host}:{port}/{db}"
print("DB URL (masked):", db_url.replace(pw, "****"))

try:
    engine = create_engine(db_url)
    with engine.connect() as conn:
        r = conn.execute(text("SELECT DATABASE(), USER()")).fetchone()
        print("Connected OK ->", r)
except SQLAlchemyError as e:
    print("SQLAlchemyError:", repr(e))
except Exception as e:
    print("Other error:", repr(e))
