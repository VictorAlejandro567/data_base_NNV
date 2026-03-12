import pandas as pd
from psycopg2.extras import execute_values
from ETL.connection import get_connection


def load_road_segment():
    df = pd.read_parquet("data/road_segment.parquet")

    df["created_at"] = pd.to_datetime(df["created_at"], unit="ms", errors="coerce")
    df["speed_limit_kph"] = df["speed_limit_kph"].where(df["speed_limit_kph"].notna(), None)
    df["is_oneway"] = df["is_oneway"].where(df["is_oneway"].notna(), None)
    df["direction"] = df["direction"].where(df["direction"].notna(), None)

    data = [
        (
            int(row["road_id"]),
            row["name"],
            int(row["speed_limit_kph"]) if row["speed_limit_kph"] is not None else None,
            row["geom"],
            row["created_at"].to_pydatetime() if pd.notna(row["created_at"]) else None,
            bool(row["is_oneway"]) if row["is_oneway"] is not None else None,
            row["direction"],
        )
        for _, row in df.iterrows()
    ]

    conn = get_connection()
    cur = conn.cursor()

    query = """
        INSERT INTO road_segment (
            road_id,
            name,
            speed_limit_kph,
            geom,
            created_at,
            is_oneway,
            direction
        )
        VALUES %s
        ON CONFLICT (road_id) DO NOTHING
    """

    template = """
        (
            %s,
            %s,
            %s,
            ST_GeomFromEWKB(decode(%s, 'hex')),
            %s,
            %s,
            %s
        )
    """

    execute_values(cur, query, data, template=template, page_size=5000)

    conn.commit()
    cur.close()
    conn.close()

    print("road_segment loaded")