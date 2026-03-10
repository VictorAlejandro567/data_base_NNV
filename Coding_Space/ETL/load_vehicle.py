import pandas as pd
from ETL.connection import get_connection


def load_vehicle():
    df = pd.read_parquet("data/vehicle.parquet")

    conn = get_connection()
    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute(
            """
            INSERT INTO vehicle (
                vehicle_id,
                vehicle_type_id,
                vehicle_status_id,
                plate_number,
                make,
                model,
                year,
                created_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (vehicle_id) DO NOTHING
            """,
            (
                int(row["vehicle_id"]),
                int(row["vehicle_type_id"]),
                int(row["vehicle_status_id"]),
                row["plate_number"],
                row["make"],
                row["model"],
                int(row["year"]),
                None if pd.isna(row["created_at"]) else row["created_at"]
            )
        )

    conn.commit()
    cur.close()
    conn.close()

    print("vehicle loaded")