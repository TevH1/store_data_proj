import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

def get_engine():
    load_dotenv(override=True)
    # Homebrew socket auth for user 'twh'
    dsn = os.getenv("POSTGRES_URL", "postgresql+psycopg2://twh@/olist?host=/tmp")
    return create_engine(dsn, future=True)
