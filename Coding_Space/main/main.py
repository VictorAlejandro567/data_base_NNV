import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from ETL.load_fuel_type import load_fuel_type
from ETL.load_vehicle_kind import load_vehicle_kind
from ETL.load_vehicle_status import load_vehicle_status
from ETL.load_vehicle_type import load_vehicle_type
from ETL.load_vehicle import load_vehicle
from ETL.load_trip import load_trip
from ETL.load_location_ping import load_location_ping



def main():

    print("Starting ETL")

    load_fuel_type()
    load_vehicle_kind()
    load_vehicle_status()
    load_vehicle_type()
    load_vehicle()
    load_trip()
    load_location_ping()
    print("Finished")


if __name__ == "__main__":
    main()

    