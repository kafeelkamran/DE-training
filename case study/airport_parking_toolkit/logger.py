# Step 3: airport_parking_toolkit/logger.py

def setup_logger(log_file):
    import logging
    logger = logging.getLogger("parking_etl")
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    if not logger.handlers:
        logger.addHandler(fh)
    return logger

logger = setup_logger("parking_etl.log")