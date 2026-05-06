"""Unit tests for explain parsing helpers used in query modeling."""

from src.query_modeling import _extract_explain_stats, _find_index_name


def test_find_index_name_nested_ixscan():
    plan = {
        "stage": "FETCH",
        "inputStage": {"stage": "IXSCAN", "indexName": "borough_complaint_compound"},
    }
    assert _find_index_name(plan) == "borough_complaint_compound"


def test_extract_explain_stats_reads_execution_stats():
    doc = {
        "queryPlanner": {
            "winningPlan": {
                "stage": "FETCH",
                "inputStage": {"stage": "IXSCAN", "indexName": "borough_1"},
            }
        },
        "executionStats": {
            "totalDocsExamined": 5000,
            "nReturned": 120,
            "executionTimeMillis": 12,
        },
    }
    s = _extract_explain_stats(doc)
    assert s["winning_stage"] == "FETCH"
    assert s["index_name"] == "borough_1"
    assert s["total_docs_examined"] == 5000
    assert s["n_returned"] == 120
    assert s["execution_time_ms"] == 12
