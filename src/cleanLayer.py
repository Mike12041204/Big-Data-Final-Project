import pandas as pd
from typing import List
import logging

from src.models import TaxiTrip

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CleanLayer:
    def __init__(self, client, db_name: str = 'raw'):
        self.client = client
        self.db = self.client[db_name]
        self.raw_collection = self.db["rawData"]
        self.clean_collection = self.db["cleanData"]

    def load_raw_data_batches(self, batch_size: int = 20000):
        logger.info("Loading raw data from MongoDB in batches...")
        cursor = self.raw_collection.find({}, {"_id": 0}).batch_size(batch_size)
        batch = []

        for record in cursor:
            batch.append(record)
            if len(batch) >= batch_size:
                yield pd.DataFrame(batch)
                batch = []

        if batch:
            yield pd.DataFrame(batch)

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Starting cleaning process for batch...")

        df = df.drop_duplicates()
        logger.info(f"After removing duplicates: {len(df)} rows")

        df = df.dropna(how='all')
        logger.info(f"After removing empty rows: {len(df)} rows")

        if "pickup_datetime" in df.columns:
            df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"], errors="coerce")

        if "dropoff_datetime" in df.columns:
            df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"], errors="coerce")

        if "trip_distance" in df.columns and "total_amount" in df.columns:
            df = df[df["trip_distance"] > 0]
            df = df[df["total_amount"] > 0]

        if "pickup_datetime" in df.columns and "dropoff_datetime" in df.columns:
            df = df.dropna(subset=["pickup_datetime", "dropoff_datetime"])
            df["trip_duration"] = (
                df["dropoff_datetime"] - df["pickup_datetime"]
            ).dt.total_seconds() / 60.0
            df = df[df["trip_duration"] > 0]
            df["pickup_hour"] = df["pickup_datetime"].dt.hour
            df["day_of_week"] = df["pickup_datetime"].dt.day_name()

        if "trip_distance" in df.columns and "trip_duration" in df.columns:
            df["speed_mph"] = df.apply(
                lambda row: row["trip_distance"] / (row["trip_duration"] / 60)
                if row["trip_duration"] > 0 else 0,
                axis=1,
            )

        if "tip_amount" in df.columns and "fare_amount" in df.columns:
            df["tip_percent"] = df.apply(
                lambda row: (row["tip_amount"] / row["fare_amount"] * 100)
                if row["fare_amount"] and row["fare_amount"] > 0 else 0,
                axis=1,
            )

        numeric_cols = df.select_dtypes(include=['number']).columns
        for col in numeric_cols:
            if df[col].isna().any():
                df[col] = df[col].fillna(df[col].mean())

        logger.info(f"Cleaning complete. Remaining rows: {len(df)}")
        return df

    def validate_data(self, df: pd.DataFrame) -> List[dict]:
        logger.info("Validating data with Pydantic...")

        valid_data = []
        errors = 0

        for record in df.to_dict("records"):
            try:
                validated = TaxiTrip(**record)
                valid_data.append(validated.dict())
            except Exception as e:
                errors += 1

        logger.info(f"Valid: {len(valid_data)}, Errors: {errors}")
        return valid_data

    def run(self):
        logger.info("Running clean layer")

        self.clean_collection.delete_many({})

        total_cleaned = 0
        batch_number = 0

        for df_batch in self.load_raw_data_batches():
            batch_number += 1
            if df_batch.empty:
                continue

            df_clean = self.clean_data(df_batch)
            validated = self.validate_data(df_clean)
            if validated:
                self.clean_collection.insert_many(validated)
                total_cleaned += len(validated)
                logger.info(f"Batch {batch_number}: inserted {len(validated)} cleaned records")

        logger.info(f"Total cleaned records inserted: {total_cleaned}")


def cleanLayer(client):
    """Function interface for cleanLayer - called from project.py"""
    print("BEGIN CLEAN LAYER")
    cleaner = CleanLayer(client)
    cleaner.run()
    print("END CLEAN LAYER")


if __name__ == "__main__":
    from pymongo import MongoClient
    client = MongoClient('mongodb://localhost:27017/')
    cleanLayer(client)