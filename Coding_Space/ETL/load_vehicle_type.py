import pandas as pd
from ETL.connection import get_connection


def load_vehicle_type():
    df = pd.read_parquet("data/vehicle_type.parquet")

    conn = get_connection()
    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute(
            """
            INSERT INTO vehicle_type (
                id,
                vehicle_kind_id,
                fuel_type_id,
                emissions_rating,
                created_at
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
            """,
            (
                int(row["id"]),
                int(row["vehicle_kind_id"]),
                int(row["fuel_type_id"]),
                None if pd.isna(row["emissions_rating"]) else float(row["emissions_rating"]),
                None if pd.isna(row["created_at"]) else row["created_at"]
            )
        )

    conn.commit()
    cur.close()
    conn.close()

    print("vehicle_type loaded")