import pandas as pd
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(
    filename='parking_etl.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)




def clean_and_validate(df):
    logging.info("Starting data validation and cleaning")

    # Convert entry_time and exit_time to datetime
    for col in ['entry_time', 'exit_time']:
        if col in df.columns:
            logging.info(f"Converting column '{col}' to datetime")
            df[col] = pd.to_datetime(df[col], errors='coerce')
            if df[col].isnull().any():
                logging.error(f"Invalid datetime values found in '{col}' column")
                raise ValueError(f"Invalid datetime values found in '{col}' column")


    # Check vehicle_id column exists and has no nulls
    if 'vehicle_id' not in df.columns:
        logging.error("Missing 'vehicle_id' column")
        raise ValueError("Missing 'vehicle_id' column")

    if df['vehicle_id'].isnull().any():
        logging.error("Null values found in 'vehicle_id' column")
        raise ValueError("Missing or null values in 'vehicle_id' column")


    logging.info("Checking exit_time is not earlier than entry_time")
    invalid_times = df[(df['exit_time'].notnull()) & (df['exit_time'] < df['entry_time'])]
    if not invalid_times.empty:
        logging.error("Found exit_time earlier than entry_time")
        raise ValueError("exit_time cannot be earlier than entry_time")


    # Validate paid_amount non-negative
    if 'paid_amount' in df.columns:
        logging.info("Validating paid_amount column for negative values")
        if (df['paid_amount'] < 0).any():
            logging.error("Negative values found in paid_amount")
            raise ValueError("paid_amount cannot have negative values")


    logging.info("Forward filling missing values")
    df = df.ffill()

    logging.info("Data cleaning and validation completed successfully")
    return df


def ETL(input,output):

    logging.info("Starting ETL process")
    logging.info(f"Reading input CSV: {input}")
    df=pd.read_csv(input)

    df=clean_and_validate(df)

    logging.info(f"Writing cleaned data to Parquet: {output}")
    df.to_parquet(output, engine='pyarrow', index=False, coerce_timestamps='us')


    logging.info("Reading back from Parquet for verification")
    table = pd.read_parquet(output)

    logging.info("ETL process completed successfully")
    print(table.head())