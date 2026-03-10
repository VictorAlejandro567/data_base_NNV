import pandas as pd
from ETL.connection import get_connection


def load_vehicle_status():
    df = pd.read_parquet("data/vehicle_status.parquet")

    conn = get_connection()
    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute(
            """
            INSERT INTO vehicle_status (vehicle_status_id, name)
            VALUES (%s, %s)
            ON CONFLICT (vehicle_status_id) DO NOTHING
            """,
            (int(row["vehicle_status_id"]), row["name"])
        )

    conn.commit()
    cur.close()
    conn.close()

    print("vehicle_status loaded")