import pandas as pd
from psycopg2.extras import execute_values
from ETL.connection import get_connection


def load_location_ping():
    df = pd.read_parquet("data/location_ping.parquet")

    df["ts"] = pd.to_datetime(df["ts"], unit="ms", errors="coerce")
    df["speed_kph"] = df["speed_kph"].where(df["speed_kph"].notna(), None)
    df["heading_deg"] = df["heading_deg"].where(df["heading_deg"].notna(), None)

    data = [
        (
            int(row["ping_id"]),
            int(row["vehicle_id"]),
            row["ts"].to_pydatetime() if pd.notna(row["ts"]) else None,
            row["geom"],
            float(row["speed_kph"]) if row["speed_kph"] is not None else None,
            float(row["heading_deg"]) if row["heading_deg"] is not None else None,
        )
        for _, row in df.iterrows()
    ]

    conn = get_connection()
    cur = conn.cursor()

    query = """
        INSERT INTO location_ping (
            ping_id,
            vehicle_id,
            ts,
            geom,
            speed_kph,
            heading_deg
        )
        VALUES %s
        ON CONFLICT (ping_id) DO NOTHING
    """

    template = """
        (
            %s,
            %s,
            %s,
            ST_GeomFromEWKB(decode(%s, 'hex')),
            %s,
            %s
        )
    """

    execute_values(cur, query, data, template=template, page_size=5000)

    conn.commit()
    cur.close()
    conn.close()

    print("location_ping loaded")