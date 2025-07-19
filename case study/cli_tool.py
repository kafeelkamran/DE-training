import argparse
import logging
import os
import pandas as pd
from airport_parking_toolkit.validation import validate_parking_data
from airport_parking_toolkit.logger import setup_logger

logger = setup_logger("parking_etl.log")

def load_csv_files(events_path, vehicles_path, zones_path):
    try:
        events_df = pd.read_csv(events_path, parse_dates=["entry_time", "exit_time"])
        vehicles_df = pd.read_csv(vehicles_path)
        zones_df = pd.read_csv(zones_path)
        logger.info("CSV files loaded successfully.")
        return events_df, vehicles_df, zones_df
    except Exception as e:
        logger.error(f"Error loading CSVs: {e}")
        raise

def write_parquet(df, output_path, name):
    try:
        df.to_parquet(os.path.join(output_path, f"{name}.parquet"), index=False)
        logger.info(f"{name} written to Parquet successfully.")
    except Exception as e:
        logger.error(f"Failed to write {name}: {e}")
        raise

def main():
    parser = argparse.ArgumentParser(description="Airport Parking ETL Tool")
    parser.add_argument("--events", required=True, help="Path to parking_events.csv")
    parser.add_argument("--vehicles", required=True, help="Path to vehicles.csv")
    parser.add_argument("--zones", required=True, help="Path to parking_zones.csv")
    parser.add_argument("--output", required=True, help="Output folder for Parquet files")
    args = parser.parse_args()

    logger.info("ETL Job Started")
    events_df, vehicles_df, zones_df = load_csv_files(args.events, args.vehicles, args.zones)

    # Validate data
    events_df = validate_parking_data(events_df)

    # Write Parquet
    write_parquet(events_df, args.output, "parking_events")
    write_parquet(vehicles_df, args.output, "vehicles")
    write_parquet(zones_df, args.output, "parking_zones")
    logger.info("ETL Job Completed Successfully")

if __name__ == "__main__":
    main()
