import pandas as pd
from ETL.connection import get_connection


def load_trip():
    df = pd.read_parquet("data/trip.parquet")

    conn = get_connection()
    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute(
            """
            INSERT INTO trip (
                trip_id,
                vehicle_id,
                start_ts,
                end_ts,
                start_geom,
                end_geom,
                distance_km
            )
            VALUES (
                %s,
                %s,
                %s,
                %s,
                ST_GeomFromEWKB(decode(%s, 'hex')),
                ST_GeomFromEWKB(decode(%s, 'hex')),
                %s
            )
            ON CONFLICT (trip_id) DO NOTHING
            """,
            (
                int(row["trip_id"]),
                int(row["vehicle_id"]),
                None if pd.isna(row["start_ts"]) else pd.to_datetime(row["start_ts"], unit="ms"),
                None if pd.isna(row["end_ts"]) else pd.to_datetime(row["end_ts"], unit="ms"),
                row["start_geom"],
                row["end_geom"],
                None if pd.isna(row["distance_km"]) else float(row["distance_km"])
            )
        )

    conn.commit()
    cur.close()
    conn.close()

    print("trip loaded")