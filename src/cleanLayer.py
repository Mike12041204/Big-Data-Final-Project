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

    def load_raw_data(self) -> pd.DataFrame:
        logger.info("Loading raw data from MongoDB...")
        data = list(self.raw_collection.find({}, {"_id": 0}))
        df = pd.DataFrame(data)
        logger.info(f"Loaded {len(df)} rows")
        return df

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Starting cleaning process...")

        # Basic cleaning steps that work with any data
        # 1. Remove duplicates
        df = df.drop_duplicates()
        logger.info(f"After removing duplicates: {len(df)} rows")

        # 2. Remove rows with all NaN values
        df = df.dropna(how='all')
        logger.info(f"After removing empty rows: {len(df)} rows")

        # 3. For numeric columns, handle missing values
        numeric_cols = df.select_dtypes(include=['number']).columns
        for col in numeric_cols:
            df[col] = df[col].fillna(df[col].mean())

        # 4. Taxi-specific cleaning (if those columns exist)
        if "trip_distance" in df.columns and "total_amount" in df.columns:
            df = df[df["trip_distance"] > 0]
            df = df[df["total_amount"] > 0]

        if "pickup_datetime" in df.columns and "dropoff_datetime" in df.columns:
            df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"], errors="coerce")
            df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"], errors="coerce")
            df = df.dropna(subset=["pickup_datetime", "dropoff_datetime"])

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

    def save_clean_data(self, data: List[dict]):
        logger.info("Saving cleaned data to MongoDB...")

        self.clean_collection.delete_many({})

        if data:
            self.clean_collection.insert_many(data)

        logger.info(f"Inserted {len(data)} cleaned records")

    def run(self):
        df = self.load_raw_data()
        if len(df) > 0:
            df_clean = self.clean_data(df)
            validated = self.validate_data(df_clean)
            self.save_clean_data(validated)
        else:
            logger.warning("No raw data found to clean")


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