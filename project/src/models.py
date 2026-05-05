from pydantic import BaseModel, Field, field_validator, ConfigDict
from datetime import datetime
from typing import Optional

class ComplaintRecord(BaseModel):
    # Mapping based on your RAW data sample (which uses snake_case keys)
    unique_key: str = Field(alias="unique_key")
    agency: str = Field(alias="agency")
    complaint_type: str = Field(alias="complaint_type")
    descriptor: Optional[str] = Field(None, alias="descriptor")
    channel_type: str = Field(alias="open_data_channel_type")
    status: str = Field(alias="status")
    
    borough: str = Field(alias="borough")
    zip_code: Optional[str] = Field("00000", alias="incident_zip")
    
    # Coordinates come in as strings in your raw data
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    
    # Dates in your sample: '2026-05-01T02:06:03.000'
    created_date: datetime = Field(alias="created_date")

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    @field_validator("created_date", mode="before")
    @classmethod
    def parse_nyc_date(cls, v):
        if isinstance(v, str):
            # Handles '2026-05-01T02:06:03.000' or similar ISO formats
            try:
                return datetime.fromisoformat(v.replace("Z", ""))
            except ValueError:
                # Fallback for the old AM/PM format if some rows differ
                return datetime.strptime(v, "%m/%d/%Y %I:%M:%S %p")
        return v

    @field_validator("latitude", "longitude", mode="before")
    @classmethod
    def parse_coords(cls, v):
        if v is None or v == "" or v == "N/A":
            return 0.0
        try:
            return float(v)
        except (ValueError, TypeError):
            return 0.0