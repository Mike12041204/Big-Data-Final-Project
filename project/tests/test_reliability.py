import pytest
from unittest.mock import Mock, patch
from pymongo.errors import ServerSelectionTimeoutError
from src.utils.reliability import BadRecordLogger, retry_on_failure, CheckpointManager

def test_bad_record_logger():
    """Test that bad records are logged properly."""
    mock_db = Mock()
    mock_collection = Mock()
    mock_db.__getitem__ = Mock(return_value=mock_collection)

    logger = BadRecordLogger(mock_db)

    # Test logging a bad record
    bad_record = {"unique_key": "test123", "invalid_field": "test"}
    logger.log_bad_record(bad_record, "Validation error", "validation_error")

    # Verify insert_one was called
    mock_collection.insert_one.assert_called_once()
    call_args = mock_collection.insert_one.call_args[0][0]

    assert call_args["record_id"] == "test123"
    assert call_args["error_type"] == "validation_error"
    assert "original_record" in call_args
    assert "timestamp" in call_args

def test_retry_decorator_success():
    """Test that retry decorator works on successful calls."""
    call_count = 0

    @retry_on_failure(max_retries=2)
    def successful_function():
        nonlocal call_count
        call_count += 1
        return "success"

    result = successful_function()
    assert result == "success"
    assert call_count == 1

def test_retry_decorator_failure():
    """Test that retry decorator retries on failure."""
    call_count = 0

    @retry_on_failure(max_retries=2, delay=0.01)
    def failing_function():
        nonlocal call_count
        call_count += 1
        raise ServerSelectionTimeoutError("Connection failed")

    with pytest.raises(ServerSelectionTimeoutError):
        failing_function()

    assert call_count == 3  # Initial call + 2 retries

def test_checkpoint_manager():
    """Test checkpoint save and retrieval."""
    mock_db = Mock()
    mock_collection = Mock()
    mock_db.__getitem__ = Mock(return_value=mock_collection)

    # Mock find_one to return a checkpoint
    mock_collection.find_one.return_value = {
        "stage": "test_stage",
        "processed_count": 1000,
        "timestamp": 1234567890
    }

    manager = CheckpointManager(mock_db)

    # Test getting checkpoint
    checkpoint = manager.get_checkpoint("test_stage")
    assert checkpoint["processed_count"] == 1000

    # Test saving checkpoint
    manager.save_checkpoint("test_stage", "last_id", 2000, {"extra": "data"})

    # Verify replace_one was called
    mock_collection.replace_one.assert_called_once()
    call_args = mock_collection.replace_one.call_args
    doc = call_args[0][1]  # The replacement document

    assert doc["stage"] == "test_stage"
    assert doc["processed_count"] == 2000
    assert doc["last_processed_id"] == "last_id"
    assert doc["metadata"]["extra"] == "data"