import pandas as pd
from ETL.connection import get_connection

def load_fuel_type():

    df = pd.read_parquet("data/fuel_type.parquet")

    conn = get_connection()
    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute(
            """
            INSERT INTO fuel_type (fuel_type_id, name)
            VALUES (%s, %s)
            ON CONFLICT (fuel_type_id) DO NOTHING
            """,
            (int(row["fuel_type_id"]), row["name"])
        )

    conn.commit()
    cur.close()
    conn.close()

    print("fuel_type loaded")