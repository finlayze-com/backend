import os
import re
import pandas as pd

from dotenv import load_dotenv
from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.dialects.postgresql import insert


load_dotenv()

DB_URL = os.getenv("DB_URL")
TABLE_NAME = "cot_disaggregated_futures_only"

if not DB_URL:
    raise ValueError("DB_URL is not set in .env")

SYNC_DB_URL = DB_URL.replace("postgresql+asyncpg://", "postgresql+psycopg2://")
engine = create_engine(SYNC_DB_URL)


TEXT_COLUMNS = {
    "market_and_exchange_names",
    "cftc_contract_market_code",
    "cftc_market_code",
    "cftc_region_code",
    "cftc_commodity_code",
    "contract_units",
    "cftc_contract_market_code_quotes",
    "cftc_market_code_quotes",
    "cftc_commodity_code_quotes",
    "cftc_subgroup_code",
    "futonly_or_combined",
}

CONFLICT_COLS = [
    "cftc_contract_market_code",
    "cftc_market_code",
    "cftc_commodity_code",
    "as_of_date_in_form_yymmdd",
    "futonly_or_combined",
]

# عددهای بزرگ‌تر از این برای COT غیرواقعی‌اند و باعث bigint overflow می‌شوند
MAX_REASONABLE_COT_VALUE = 1_000_000_000


def normalize_col(col: str) -> str:
    col = col.strip().strip('"')
    col = re.sub(r"[^a-zA-Z0-9]+", "_", col)
    col = re.sub(r"_+", "_", col)
    return col.strip("_").lower()


def load_db_columns() -> set[str]:
    query = text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = :table_name
    """)

    with engine.connect() as conn:
        rows = conn.execute(query, {"table_name": TABLE_NAME}).fetchall()

    return {r[0] for r in rows}


def sa_table(table_name: str):
    metadata = MetaData()
    return Table(table_name, metadata, autoload_with=engine)


def clean_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if col in TEXT_COLUMNS:
            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
                .replace({"": None, "nan": None, "NaN": None, "None": None})
            )
    return df


def clean_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if col in TEXT_COLUMNS:
            continue

        if col.startswith("report_date"):
            continue

        df[col] = (
            df[col]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.strip()
            .replace({
                "": None,
                "nan": None,
                "NaN": None,
                "None": None,
                ".": None,
                "-": None,
            })
        )

        df[col] = pd.to_numeric(df[col], errors="coerce")

        # حذف مقدارهای غیرواقعی که باعث bigint overflow می‌شوند
        df.loc[df[col].abs() > MAX_REASONABLE_COT_VALUE, col] = None

    return df


def prepare_dataframe(file_path: str, db_cols: set[str]) -> pd.DataFrame:
    df = pd.read_csv(file_path, low_memory=False)

    df.columns = [normalize_col(c) for c in df.columns]

    if "report_date_as_mm_dd_yyyy" in df.columns:
        df["report_date_as_yyyy_mm_dd"] = pd.to_datetime(
            df["report_date_as_mm_dd_yyyy"],
            errors="coerce",
        ).dt.date
        df = df.drop(columns=["report_date_as_mm_dd_yyyy"])

    elif "report_date_as_yyyy_mm_dd" in df.columns:
        df["report_date_as_yyyy_mm_dd"] = pd.to_datetime(
            df["report_date_as_yyyy_mm_dd"],
            errors="coerce",
        ).dt.date

    df = clean_text_columns(df)
    df = clean_numeric_columns(df)

    matched_cols = [c for c in df.columns if c in db_cols]
    df = df[matched_cols]

    missing_conflicts = [c for c in CONFLICT_COLS if c not in df.columns]
    if missing_conflicts:
        raise ValueError(
            f"Missing conflict columns in file {file_path}: {missing_conflicts}"
        )

    df = df.astype(object).where(pd.notnull(df), None)
    df = df.drop_duplicates(subset=CONFLICT_COLS, keep="last")

    return df


def upsert_dataframe(df: pd.DataFrame, batch_size: int = 20):
    if df.empty:
        print("No rows to insert.")
        return

    table = sa_table(TABLE_NAME)
    total_rows = len(df)

    for start in range(0, total_rows, batch_size):
        end = start + batch_size
        batch_df = df.iloc[start:end].copy()

        # safety clean again before insert
        for col in batch_df.columns:
            if col in TEXT_COLUMNS or col.startswith("report_date"):
                continue

            batch_df[col] = pd.to_numeric(batch_df[col], errors="coerce")
            batch_df.loc[
                batch_df[col].abs() > MAX_REASONABLE_COT_VALUE,
                col
            ] = None

        batch_df = batch_df.astype(object).where(pd.notnull(batch_df), None)
        records = batch_df.to_dict(orient="records")

        stmt = insert(table).values(records)

        update_cols = {
            c: getattr(stmt.excluded, c)
            for c in batch_df.columns
            if c not in CONFLICT_COLS and c != "id"
        }

        stmt = stmt.on_conflict_do_update(
            index_elements=CONFLICT_COLS,
            set_=update_cols,
        )

        with engine.begin() as conn:
            conn.execute(stmt)

        print(f"Upserted rows {start + 1} to {min(end, total_rows)} of {total_rows}")


def import_cot_file(file_path: str, db_cols: set[str]):
    print(f"Reading: {file_path}")

    df = prepare_dataframe(file_path, db_cols)

    print(f"Rows: {len(df)} | Columns matched: {len(df.columns)}")

    upsert_dataframe(df)


def import_cot_folder(folder_path: str):
    if not os.path.isdir(folder_path):
        raise FileNotFoundError(f"Folder not found: {folder_path}")

    files = [
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if f.lower().endswith((".txt", ".csv"))
    ]

    if not files:
        print(f"No .txt or .csv files found in: {folder_path}")
        return

    db_cols = load_db_columns()

    print(f"DB columns found: {len(db_cols)}")
    print(f"Files found: {len(files)}")

    for file_path in sorted(files):
        import_cot_file(file_path, db_cols)


if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    HISTORY_DIR = os.path.join(BASE_DIR, "history")

    print(f"Loading files from: {HISTORY_DIR}")

    import_cot_folder(HISTORY_DIR)