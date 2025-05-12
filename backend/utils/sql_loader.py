# فایل: backend/utils/sql_loader.py

def load_sql(name: str) -> str:
    with open(f"backend/sql/{name}.sql", encoding="utf-8") as f:
        return f.read()
