import pandas as pd
from psycopg2.extras import execute_values
from ETL.connection import get_connection


def load_trip():
    df = pd.read_parquet("data/trip.parquet")

    df["start_ts"] = pd.to_datetime(df["start_ts"], unit="ms", errors="coerce")
    df["end_ts"] = pd.to_datetime(df["end_ts"], unit="ms", errors="coerce")
    df["distance_km"] = df["distance_km"].where(df["distance_km"].notna(), None)

    data = [
        (
            int(row["trip_id"]),
            int(row["vehicle_id"]),
            row["start_ts"].to_pydatetime() if pd.notna(row["start_ts"]) else None,
            row["end_ts"].to_pydatetime() if pd.notna(row["end_ts"]) else None,
            row["start_geom"],
            row["end_geom"],
            float(row["distance_km"]) if row["distance_km"] is not None else None,
        )
        for _, row in df.iterrows()
    ]

    conn = get_connection()
    cur = conn.cursor()

    query = """
        INSERT INTO trip (
            trip_id,
            vehicle_id,
            start_ts,
            end_ts,
            start_geom,
            end_geom,
            distance_km
        )
        VALUES %s
        ON CONFLICT (trip_id) DO NOTHING
    """

    template = """
        (
            %s,
            %s,
            %s,
            %s,
            ST_GeomFromEWKB(decode(%s, 'hex')),
            ST_GeomFromEWKB(decode(%s, 'hex')),
            %s
        )
    """

    execute_values(cur, query, data, template=template, page_size=1000)

    conn.commit()
    cur.close()
    conn.close()

    print("trip loaded")