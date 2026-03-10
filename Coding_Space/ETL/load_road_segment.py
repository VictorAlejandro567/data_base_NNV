import pandas as pd
from ETL.connection import get_connection


def load_road_segment():
    df = pd.read_parquet("data/road_segment.parquet")

    conn = get_connection()
    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute(
            """
            INSERT INTO road_segment (
                road_id,
                name,
                speed_limit_kph,
                geom,
                created_at,
                is_oneway,
                direction
            )
            VALUES (
                %s,
                %s,
                %s,
                ST_GeomFromEWKB(decode(%s, 'hex')),
                %s,
                %s,
                %s
            )
            ON CONFLICT (road_id) DO NOTHING
            """,
            (
                int(row["road_id"]),
                row["name"],
                None if pd.isna(row["speed_limit_kph"]) else int(row["speed_limit_kph"]),
                row["geom"],
                None if pd.isna(row["created_at"]) else pd.to_datetime(row["created_at"], unit="ms"),
                None if pd.isna(row["is_oneway"]) else bool(row["is_oneway"]),
                row["direction"]
            )
        )

    conn.commit()
    cur.close()
    conn.close()

    print("road_segment loaded")