# Step 2: airport_parking_toolkit/validation.py

def validate_parking_data(df):
    import pandas as pd
    from airport_parking_toolkit.logger import logger

    original_count = len(df)
    df = df.dropna(subset=["vehicle_id", "entry_time", "exit_time", "paid_amount"])
    df = df[df["exit_time"] >= df["entry_time"]]	
    df = df[df["paid_amount"] >= 0]
    filtered_count = len(df)

    logger.info(f"Validation complete. Dropped {original_count - filtered_count} invalid records.")
    return df