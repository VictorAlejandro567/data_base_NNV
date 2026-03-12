import pandas as pd
from psycopg2.extras import execute_values
from ETL.connection import get_connection


def load_parking_area():
    df = pd.read_parquet("data/parking_area.parquet")

    df["created_at"] = pd.to_datetime(df["created_at"], unit="ms", errors="coerce")
    df["capacity"] = df["capacity"].where(df["capacity"].notna(), None)

    data = [
        (
            int(row["parking_area_id"]),
            row["name"],
            int(row["capacity"]) if row["capacity"] is not None else None,
            row["geom"],
            row["created_at"].to_pydatetime() if pd.notna(row["created_at"]) else None,
        )
        for _, row in df.iterrows()
    ]

    conn = get_connection()
    cur = conn.cursor()

    query = """
        INSERT INTO parking_area (
            parking_area_id,
            name,
            capacity,
            geom,
            created_at
        )
        VALUES %s
        ON CONFLICT (parking_area_id) DO NOTHING
    """

    template = """
        (
            %s,
            %s,
            %s,
            ST_GeomFromEWKB(decode(%s, 'hex')),
            %s
        )
    """

    execute_values(cur, query, data, template=template, page_size=len(data))

    conn.commit()
    cur.close()
    conn.close()

    print("parking_area loaded")