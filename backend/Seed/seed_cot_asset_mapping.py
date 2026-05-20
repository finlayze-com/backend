import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text


load_dotenv()

DB_URL = os.getenv("DB_URL")

if not DB_URL:
    raise ValueError("DB_URL is not set")


SYNC_DB_URL = DB_URL.replace(
    "postgresql+asyncpg://",
    "postgresql+psycopg2://"
)

engine = create_engine(SYNC_DB_URL)


def classify_contract(name: str):

    n = str(name).upper()

    # ------------------
    # METALS
    # ------------------

    if (
        "GOLD" in n
        and "FINANCIAL" not in n
    ):
        return (
            "METALS",
            "XAU",
            "Gold",
            "precious_metals",
            1
        )

    if (
        "SILVER" in n
        and "FINANCIAL" not in n
    ):
        return (
            "METALS",
            "XAG",
            "Silver",
            "precious_metals",
            2
        )

    if "COPPER" in n:
        return (
            "METALS",
            "COPPER",
            "Copper",
            "industrial_metals",
            3
        )

    if "PLATINUM" in n:
        return (
            "METALS",
            "PLAT",
            "Platinum",
            "precious_metals",
            4
        )

    if "PALLADIUM" in n:
        return (
            "METALS",
            "PALL",
            "Palladium",
            "precious_metals",
            5
        )

    if "ALUMINIUM" in n:
        return (
            "METALS",
            "AL",
            "Aluminium",
            "industrial_metals",
            6
        )

    # ------------------
    # PETROLEUM
    # ------------------

    if (
        "CRUDE" in n
        and "INDEX" not in n
    ):
        return (
            "PETROLEUM",
            "WTI",
            name,
            "crude",
            1
        )

    if "BRENT" in n:
        return (
            "PETROLEUM",
            "BRENT",
            name,
            "crude",
            2
        )

    if (
        "RBOB" in n
        or "GASOLINE" in n
    ):
        return (
            "PETROLEUM",
            "RBOB",
            name,
            "products",
            3
        )

    if (
        "HEATING OIL" in n
        or "ULSD" in n
        or "DIESEL" in n
    ):
        return (
            "PETROLEUM",
            "DIST",
            name,
            "products",
            4
        )

    if "FUEL OIL" in n:
        return (
            "PETROLEUM",
            "FUEL",
            name,
            "products",
            5
        )

    # ------------------
    # NATURAL GAS
    # ------------------

    if (
        "NATURAL GAS" in n
        or "HENRY HUB" in n
    ):
        return (
            "NATURAL_GAS",
            "NG",
            name,
            "gas",
            1
        )

    # ------------------
    # ELECTRICITY
    # ------------------

    if (
        "ELECTRICITY" in n
        or "PJM" in n
        or "ERCOT" in n
        or "MISO" in n
        or "ISO" in n
    ):
        return (
            "ELECTRICITY",
            None,
            name,
            "power",
            1
        )

    # ------------------
    # STOCK INDEX
    # ------------------

    if (
        "S&P" in n
        or "SP 500" in n
        or "NASDAQ" in n
        or "RUSSELL" in n
        or "NIKKEI" in n
        or "MSCI" in n
    ):
        return (
            "STOCK_INDEX",
            None,
            name,
            "index",
            1
        )

    # ------------------
    # AGRICULTURE
    # ------------------

    AG_RULES = [
        "CORN",
        "WHEAT",
        "SOY",
        "COTTON",
        "COFFEE",
        "SUGAR",
        "COCOA",
        "CATTLE",
        "HOGS",
        "OATS",
        "RICE",
    ]

    for x in AG_RULES:

        if x in n:

            return (
                "AGRICULTURE",
                None,
                name,
                "ag",
                1
            )

    # ------------------
    # OTHER
    # ------------------

    return (
        "OTHER",
        None,
        name,
        "unclassified",
        999
    )


def seed_mapping():

    select_sql = text("""
        SELECT
            cftc_contract_market_code,
            MAX(market_and_exchange_names)
                AS market_and_exchange_names
        FROM public.cot_disaggregated_futures_only
        GROUP BY
            cftc_contract_market_code
        ORDER BY
            cftc_contract_market_code
    """)

    upsert_sql = text("""
        INSERT INTO cot_asset_mapping
        (
            cftc_contract_market_code,
            market_and_exchange_names,
            asset_group,
            asset_code,
            asset_name,
            asset_subgroup,
            dashboard_order,
            is_active
        )

        VALUES
        (
            :code,
            :market,
            :group_name,
            :asset_code,
            :asset_name,
            :subgroup,
            :ord,
            true
        )

        ON CONFLICT
        (
            cftc_contract_market_code
        )

        DO UPDATE SET

        market_and_exchange_names
            =
        EXCLUDED.market_and_exchange_names,

        asset_group
            =
        EXCLUDED.asset_group,

        asset_code
            =
        EXCLUDED.asset_code,

        asset_name
            =
        EXCLUDED.asset_name,

        asset_subgroup
            =
        EXCLUDED.asset_subgroup,

        dashboard_order
            =
        EXCLUDED.dashboard_order,

        is_active=true
    """)

    with engine.begin() as conn:

        contracts = (
            conn.execute(select_sql)
            .mappings()
            .all()
        )

        for row in contracts:

            (
                group_name,
                asset_code,
                asset_name,
                subgroup,
                ord_,
            ) = classify_contract(
                row["market_and_exchange_names"]
            )

            conn.execute(
                upsert_sql,
                {
                    "code":
                        row[
                            "cftc_contract_market_code"
                        ],

                    "market":
                        row[
                            "market_and_exchange_names"
                        ],

                    "group_name":
                        group_name,

                    "asset_code":
                        asset_code,

                    "asset_name":
                        asset_name,

                    "subgroup":
                        subgroup,

                    "ord":
                        ord_,
                }
            )

    print(
        f"Seeded {len(contracts)} mappings."
    )


if __name__ == "__main__":
    seed_mapping()