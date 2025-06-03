
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from backend.stocks.models import Base

DATABASE_URL = "postgresql://myuser:mypass@localhost/postgres1"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    Base.metadata.create_all(bind=engine)

if __name__ == "__main__":
    init_db()
