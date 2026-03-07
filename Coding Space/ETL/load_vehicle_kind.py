import pandas as pd
from ETL.connection import get_connection


def load_vehicle_kind():
    df = pd.read_parquet("data/vehicle_kind.parquet")

    conn = get_connection()
    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute(
            """
            INSERT INTO vehicle_kind (vehicle_kind_id, name)
            VALUES (%s, %s)
            ON CONFLICT (vehicle_kind_id) DO NOTHING
            """,
            (int(row["vehicle_kind_id"]), row["name"])
        )

    conn.commit()
    cur.close()
    conn.close()

    print("vehicle_kind loaded")