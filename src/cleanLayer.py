<<<<<<< HEAD
import pandas as pd
from typing import List
import logging
import gc

from src.models import TaxiTrip

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CleanLayer:
    def __init__(self, client, db_name: str = 'raw'):
        self.client = client
        self.db = self.client[db_name]
        self.raw_collection = self.db["rawData"]
        self.clean_collection = self.db["cleanData"]

    def load_raw_data_batches(self, batch_size: int = 50000):
        logger.info(f"Loading raw data from MongoDB in batches of {batch_size}...")
        cursor = self.raw_collection.find({}, {"_id": 0}).batch_size(batch_size)
        batch = []

        for record in cursor:
            batch.append(record)
            if len(batch) >= batch_size:
                yield pd.DataFrame(batch)
                batch = []
                gc.collect()  # Force garbage collection after yielding

        if batch:
            yield pd.DataFrame(batch)
            gc.collect()

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

        # Vectorized filtering for positive values
        if "trip_distance" in df.columns and "total_amount" in df.columns:
            df = df[(df["trip_distance"] > 0) & (df["total_amount"] > 0)]

        # Process datetime-based features
        if "pickup_datetime" in df.columns and "dropoff_datetime" in df.columns:
            df = df.dropna(subset=["pickup_datetime", "dropoff_datetime"])
            df["trip_duration"] = (
                df["dropoff_datetime"] - df["pickup_datetime"]
            ).dt.total_seconds() / 60.0
            df = df[df["trip_duration"] > 0]
            df["pickup_hour"] = df["pickup_datetime"].dt.hour
            df["day_of_week"] = df["pickup_datetime"].dt.day_name()

        # Vectorized speed calculation (no apply)
        if "trip_distance" in df.columns and "trip_duration" in df.columns:
            df["speed_mph"] = 0.0
            mask = df["trip_duration"] > 0
            df.loc[mask, "speed_mph"] = df.loc[mask, "trip_distance"] / (df.loc[mask, "trip_duration"] / 60)

        # Vectorized tip percent calculation (no apply)
        if "tip_amount" in df.columns and "fare_amount" in df.columns:
            df["tip_percent"] = 0.0
            mask = df["fare_amount"] > 0
            df.loc[mask, "tip_percent"] = (df.loc[mask, "tip_amount"] / df.loc[mask, "fare_amount"]) * 100

        # Efficient NaN filling for numeric columns
        numeric_cols = df.select_dtypes(include=['number']).columns
        for col in numeric_cols:
            if df[col].isna().any():
                df[col].fillna(df[col].mean(), inplace=True)

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

        try:
            self.clean_collection.delete_many({})
            logger.info("Cleared existing clean data")
        except Exception as e:
            logger.warning(f"Could not clear existing clean data: {e}")
            logger.info("Proceeding with data cleaning...")

        total_cleaned = 0
        batch_number = 0

        for df_batch in self.load_raw_data_batches():
            batch_number += 1
            if df_batch.empty:
                continue

            try:
                df_clean = self.clean_data(df_batch)
                validated = self.validate_data(df_clean)
                if validated:
                    self.clean_collection.insert_many(validated)
                    total_cleaned += len(validated)
                    logger.info(f"Batch {batch_number}: inserted {len(validated)} cleaned records (total: {total_cleaned})")
                
                # Aggressive garbage collection
                del df_batch, df_clean, validated
                gc.collect()
            except Exception as e:
                logger.error(f"Error processing batch {batch_number}: {e}")
                gc.collect()
                continue

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
=======
import pandas as pd
from typing import List
import logging
import gc

from src.models import TaxiTrip

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CleanLayer:
    def __init__(self, client, db_name: str = 'raw'):
        self.client = client
        self.db = self.client[db_name]
        self.raw_collection = self.db["rawData"]
        self.clean_collection = self.db["cleanData"]

    def load_raw_data_batches(self, batch_size: int = 50000):
        logger.info(f"Loading raw data from MongoDB in batches of {batch_size}...")
        cursor = self.raw_collection.find({}, {"_id": 0}).batch_size(batch_size)
        batch = []

        for record in cursor:
            batch.append(record)
            if len(batch) >= batch_size:
                yield pd.DataFrame(batch)
                batch = []
                gc.collect()  # Force garbage collection after yielding

        if batch:
            yield pd.DataFrame(batch)
            gc.collect()

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

        # Vectorized filtering for positive values
        if "trip_distance" in df.columns and "total_amount" in df.columns:
            df = df[(df["trip_distance"] > 0) & (df["total_amount"] > 0)]

        # Process datetime-based features
        if "pickup_datetime" in df.columns and "dropoff_datetime" in df.columns:
            df = df.dropna(subset=["pickup_datetime", "dropoff_datetime"])
            df["trip_duration"] = (
                df["dropoff_datetime"] - df["pickup_datetime"]
            ).dt.total_seconds() / 60.0
            df = df[df["trip_duration"] > 0]
            df["pickup_hour"] = df["pickup_datetime"].dt.hour
            df["day_of_week"] = df["pickup_datetime"].dt.day_name()

        # Vectorized speed calculation (no apply)
        if "trip_distance" in df.columns and "trip_duration" in df.columns:
            df["speed_mph"] = 0.0
            mask = df["trip_duration"] > 0
            df.loc[mask, "speed_mph"] = df.loc[mask, "trip_distance"] / (df.loc[mask, "trip_duration"] / 60)

        # Vectorized tip percent calculation (no apply)
        if "tip_amount" in df.columns and "fare_amount" in df.columns:
            df["tip_percent"] = 0.0
            mask = df["fare_amount"] > 0
            df.loc[mask, "tip_percent"] = (df.loc[mask, "tip_amount"] / df.loc[mask, "fare_amount"]) * 100

        # Efficient NaN filling for numeric columns
        numeric_cols = df.select_dtypes(include=['number']).columns
        for col in numeric_cols:
            if df[col].isna().any():
                df[col].fillna(df[col].mean(), inplace=True)

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

        try:
            self.clean_collection.delete_many({})
            logger.info("Cleared existing clean data")
        except Exception as e:
            logger.warning(f"Could not clear existing clean data: {e}")
            logger.info("Proceeding with data cleaning...")

        total_cleaned = 0
        batch_number = 0

        for df_batch in self.load_raw_data_batches():
            batch_number += 1
            if df_batch.empty:
                continue

            try:
                df_clean = self.clean_data(df_batch)
                validated = self.validate_data(df_clean)
                if validated:
                    self.clean_collection.insert_many(validated)
                    total_cleaned += len(validated)
                    logger.info(f"Batch {batch_number}: inserted {len(validated)} cleaned records (total: {total_cleaned})")
                
                # Aggressive garbage collection
                del df_batch, df_clean, validated
                gc.collect()
            except Exception as e:
                logger.error(f"Error processing batch {batch_number}: {e}")
                gc.collect()
                continue

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
>>>>>>> 31a89514fe5c607ba63bb0574cb71393aba7e6a1
    cleanLayer(client)