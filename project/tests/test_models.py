import pytest
from datetime import datetime
from src.models import ComplaintRecord

def test_complaint_record_valid():
    """Test that valid raw data is correctly aliased and parsed."""
    raw_data = {
        "unique_key": "12345",
        "agency": "NYPD",
        "complaint_type": "Noise",
        "open_data_channel_type": "PHONE",
        "status": "Open",
        "borough": "BRONX",
        "incident_zip": "10451",
        "latitude": "40.81",
        "longitude": "-73.92",
        "created_date": "2026-05-01T02:06:03.000"
    }
    record = ComplaintRecord(**raw_data)
    assert record.unique_key == "12345"
    assert record.latitude == 40.81
    assert isinstance(record.created_date, datetime)

def test_date_fallback_parsing():
    """Test that the validator handles the old AM/PM format."""
    raw_data = {
        "unique_key": "1", "agency": "A", "complaint_type": "C",
        "open_data_channel_type": "O", "status": "S", "borough": "B",
        "created_date": "05/01/2026 02:06:03 AM"
    }
    record = ComplaintRecord(**raw_data)
    assert record.created_date.hour == 2

def test_coordinate_parsing_edge_cases():
    """Test that 'N/A' or empty strings become 0.0."""
    base = {
        "unique_key": "1", "agency": "A", "complaint_type": "C",
        "open_data_channel_type": "O", "status": "S", "borough": "B",
        "created_date": "2026-05-01T02:06:03.000"
    }
    
    # Test "N/A"
    rec1 = ComplaintRecord(**{**base, "latitude": "N/A"})
    assert rec1.latitude == 0.0
    
    # Test empty string
    rec2 = ComplaintRecord(**{**base, "longitude": ""})
    assert rec2.longitude == 0.0