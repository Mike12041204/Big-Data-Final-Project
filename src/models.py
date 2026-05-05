from pydantic import BaseModel, ConfigDict
from typing import Optional
from datetime import datetime

class RawData(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True, str_strip_whitespace=True)

    vendor_id: Optional[int] = None
    pickup_datetime: Optional[str] = None
    dropoff_datetime: Optional[str] = None
    passenger_count: Optional[int] = None
    trip_distance: Optional[float] = None
    rate_code: Optional[int] = None
    store_and_fwd_flag: Optional[str] = None
    payment_type: Optional[int] = None
    fare_amount: Optional[float] = None
    extra: Optional[float] = None
    mta_tax: Optional[float] = None
    tip_amount: Optional[float] = None
    tolls_amount: Optional[float] = None
    imp_surcharge: Optional[float] = None
    total_amount: Optional[float] = None
    pickup_location_id: Optional[int] = None
    dropoff_location_id: Optional[int] = None

    def to_mongo(self) -> dict:
        return self.model_dump(by_alias=True, exclude_none=True)

class TaxiTrip(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True, str_strip_whitespace=True)

    vendor_id: Optional[int] = None
    pickup_datetime: Optional[datetime] = None
    dropoff_datetime: Optional[datetime] = None
    passenger_count: Optional[int] = None
    trip_distance: Optional[float] = None
    rate_code: Optional[int] = None
    store_and_fwd_flag: Optional[str] = None
    payment_type: Optional[int] = None
    fare_amount: Optional[float] = None
    extra: Optional[float] = None
    mta_tax: Optional[float] = None
    tip_amount: Optional[float] = None
    tolls_amount: Optional[float] = None
    imp_surcharge: Optional[float] = None
    total_amount: Optional[float] = None
    pickup_location_id: Optional[int] = None
    dropoff_location_id: Optional[int] = None
    pickup_hour: Optional[int] = None
    day_of_week: Optional[str] = None
    trip_duration: Optional[float] = None
    speed_mph: Optional[float] = None
    tip_percent: Optional[float] = None

    def dict(self):
        return self.model_dump(exclude_none=True)
