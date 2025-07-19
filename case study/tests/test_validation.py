# Step 4: tests/test_validation.py

def test_validate_parking_data_valid():
    import pandas as pd
    from airport_parking_toolkit.validation import validate_parking_data

    data = {
        "vehicle_id": [1, 2],
        "entry_time": ["2023-07-01 08:00:00", "2023-07-01 09:00:00"],
        "exit_time": ["2023-07-01 10:00:00", "2023-07-01 11:00:00"],
        "paid_amount": [10.0, 15.0]
    }
    df = pd.DataFrame(data)
    df["entry_time"] = pd.to_datetime(df["entry_time"])
    df["exit_time"] = pd.to_datetime(df["exit_time"])

    result = validate_parking_data(df)
    assert len(result) == 2


def test_validate_parking_data_invalid():
    import pandas as pd
    from airport_parking_toolkit.validation import validate_parking_data

    data = {
        "vehicle_id": [1, None],
        "entry_time": ["2023-07-01 08:00:00", "2023-07-01 10:00:00"],
        "exit_time": ["2023-07-01 07:00:00", "2023-07-01 09:00:00"],
        "paid_amount": [10.0, -5.0]
    }
    df = pd.DataFrame(data)
    df["entry_time"] = pd.to_datetime(df["entry_time"])
    df["exit_time"] = pd.to_datetime(df["exit_time"])

    result = validate_parking_data(df)
    assert len(result) == 0