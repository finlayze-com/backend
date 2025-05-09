# backend/db/connection.py

from sqlalchemy import create_engine

DB_URI = "postgresql://postgres:Afiroozi12@localhost:5432/postgres1"

def get_engine():
    return create_engine(DB_URI)
