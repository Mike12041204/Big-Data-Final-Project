import pytest
from unittest.mock import MagicMock
from src.rawLayer import rawLayer
from src.cleanLayer import cleanLayer
from src.aggregateLayer import aggregateLayer

def test_placeholder():
    assert pytest is not None

def test_rawlayer_runs_without_error():
    rawLayer(MagicMock())

def test_cleanlayer_runs_without_error():
    cleanLayer(MagicMock())

def test_aggregatelayer_runs_without_error():
    aggregateLayer(MagicMock())