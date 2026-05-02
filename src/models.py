from pydantic import BaseModel, ConfigDict
from typing import Optional
from datetime import datetime

class RawData(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True, str_strip_whitespace=True)

    name: str
    category: str
    value: int

    def to_mongo(self) -> dict:
        data = self.model_dump(by_alias=True)
        return data

class TaxiTrip(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True, str_strip_whitespace=True)

    pickup_datetime: Optional[str] = None
    dropoff_datetime: Optional[str] = None
    trip_distance: Optional[float] = None
    total_amount: Optional[float] = None
    fare_amount: Optional[float] = None
    passenger_count: Optional[int] = None
    tip_amount: Optional[float] = None
    pickup_hour: Optional[int] = None
    day_of_week: Optional[str] = None
    trip_category: Optional[str] = None
    trip_duration: Optional[float] = None
    speed_mph: Optional[float] = None
    tip_percent: Optional[float] = None

    def dict(self):
        return self.model_dump()