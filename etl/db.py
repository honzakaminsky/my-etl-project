# etl/db.py
import os
import time
from sqlalchemy import create_engine, text

def get_engine():
    db = os.environ["POSTGRES_DB"]
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    host = os.environ["POSTGRES_HOST"]

    for i in range(10):
        try:
            engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:5432/{db}")
            with engine.connect() as conn:
                conn.execute(text('SELECT 1'))
            print("✅ Database connection established")
            return engine
        except Exception as e:
            print(f"⏳ Waiting for DB... ({i+1}/10): {e}")
            time.sleep(2)
    raise Exception("❌ Could not connect to the database after 10 tries.")
