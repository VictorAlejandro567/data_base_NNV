import pandas as pd
from ETL.connection import get_connection


def load_parking_area():
    df = pd.read_parquet("data/parking_area.parquet")

    conn = get_connection()
    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute(
            """
            INSERT INTO parking_area (
                parking_area_id,
                name,
                capacity,
                geom,
                created_at
            )
            VALUES (
                %s,
                %s,
                %s,
                ST_GeomFromEWKB(decode(%s, 'hex')),
                %s
            )
            ON CONFLICT (parking_area_id) DO NOTHING
            """,
            (
                int(row["parking_area_id"]),
                row["name"],
                None if pd.isna(row["capacity"]) else int(row["capacity"]),
                row["geom"],
                None if pd.isna(row["created_at"]) else pd.to_datetime(row["created_at"], unit="ms")
            )
        )

    conn.commit()
    cur.close()
    conn.close()

    print("parking_area loaded")