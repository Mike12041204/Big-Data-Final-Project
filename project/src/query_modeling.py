"""
Query modeling and performance (course Part 6).

Demonstrates:
- MongoDB indexing: compound index vs single-field plans (explain + timing).
- Storage partitioning: Hive-style Parquet layout for partition pruning in Polars.
"""
from __future__ import annotations

import os
import shutil
import time
from typing import Any

import polars as pl
from pymongo.database import Database

from src.config import CLEAN_COLLECTION
from src.utils.logger import get_logger

logger = get_logger("QueryModeling")

COMPOUND_INDEX_NAME = "borough_complaint_compound"


def _find_index_name(plan: dict[str, Any] | None) -> str | None:
    if not plan:
        return None
    if plan.get("indexName"):
        return str(plan["indexName"])
    if plan.get("inputStage"):
        return _find_index_name(plan["inputStage"])
    for sub in plan.get("inputStages") or []:
        hit = _find_index_name(sub)
        if hit:
            return hit
    return None


def _extract_explain_stats(explain_doc: dict[str, Any]) -> dict[str, Any]:
    """Pull common executionStats / queryPlanner fields from a find explain document."""
    qp = explain_doc.get("queryPlanner") or {}
    winning = qp.get("winningPlan") or {}
    es = explain_doc.get("executionStats") or {}
    return {
        "winning_stage": winning.get("stage"),
        "index_name": _find_index_name(winning),
        "total_docs_examined": es.get("totalDocsExamined"),
        "n_returned": es.get("nReturned"),
        "execution_time_ms": es.get("executionTimeMillis"),
    }


def _explain_find(db: Database, filt: dict[str, Any]) -> dict[str, Any]:
    """Run explain(executionStats) for a find on cleaned_311."""
    cmd = {
        "explain": {
            "find": CLEAN_COLLECTION,
            "filter": filt,
        },
        "verbosity": "executionStats",
    }
    return db.command(cmd)


def _pick_example_filter(db: Database) -> dict[str, Any] | None:
    """Choose a real borough + complaint_type pair so the demo hits data."""
    clean = db[CLEAN_COLLECTION]
    doc = clean.find_one(
        {"borough": {"$exists": True}, "complaint_type": {"$exists": True}},
        {"borough": 1, "complaint_type": 1, "_id": 0},
    )
    if not doc:
        return None
    return {"borough": doc["borough"], "complaint_type": doc["complaint_type"]}


def run_mongodb_index_benchmark(db: Database) -> None:
    """
    Compare query behavior with and without the compound (borough, complaint_type) index.

    Single-field indexes on borough and complaint_type remain in place so this is a
    realistic "add compound index" upgrade rather than a full collection scan.
    """
    clean = db[CLEAN_COLLECTION]
    example = _pick_example_filter(db)
    if not example:
        logger.warning("No documents in cleaned collection; skipping MongoDB index benchmark.")
        return

    logger.info("=== MongoDB: compound index benchmark ===")
    logger.info("Example filter (equality on borough + complaint_type): %s", example)

    had_compound = any(
        idx.get("name") == COMPOUND_INDEX_NAME for idx in clean.list_indexes()
    )

    def timed_find() -> tuple[float, int]:
        t0 = time.perf_counter()
        n = clean.count_documents(example)
        elapsed = time.perf_counter() - t0
        return elapsed, n

    # --- Phase A: without compound index ---
    if had_compound:
        clean.drop_index(COMPOUND_INDEX_NAME)

    explain_before = _explain_find(db, example)
    stats_before = _extract_explain_stats(explain_before)
    wall_before, count_before = timed_find()

    # --- Phase B: with compound index ---
    clean.create_index(
        [("borough", 1), ("complaint_type", 1)],
        name=COMPOUND_INDEX_NAME,
    )
    explain_after = _explain_find(db, example)
    stats_after = _extract_explain_stats(explain_after)
    wall_after, count_after = timed_find()

    logger.info(
        "Before compound index — wall_time=%.4fs count=%s planner_stage=%s index=%s "
        "docs_examined=%s exec_ms=%s",
        wall_before,
        count_before,
        stats_before.get("winning_stage"),
        stats_before.get("index_name"),
        stats_before.get("total_docs_examined"),
        stats_before.get("execution_time_ms"),
    )
    logger.info(
        "After compound index — wall_time=%.4fs count=%s planner_stage=%s index=%s "
        "docs_examined=%s exec_ms=%s",
        wall_after,
        count_after,
        stats_after.get("winning_stage"),
        stats_after.get("index_name"),
        stats_after.get("total_docs_examined"),
        stats_after.get("execution_time_ms"),
    )
    if wall_before > 0:
        logger.info(
            "Wall-clock speedup (before/after): %.2fx",
            wall_before / max(wall_after, 1e-9),
        )
    if (
        stats_before.get("total_docs_examined")
        and stats_after.get("total_docs_examined")
        and stats_after["total_docs_examined"] > 0
    ):
        logger.info(
            "Docs examined ratio (before/after): %.2fx",
            stats_before["total_docs_examined"] / stats_after["total_docs_examined"],
        )


def run_partition_pruning_demo(df: pl.DataFrame, demo_root: str) -> None:
    """
    Hive-style directory partitioning by borough so scans can prune files.

    Contrasts one monolithic Parquet file vs partitioned layout for the same filter.
    """
    if len(df) == 0:
        logger.warning("Empty DataFrame; skipping partition pruning demo.")
        return

    boroughs = df["borough"].drop_nulls().unique().to_list()
    if len(boroughs) < 2:
        logger.warning("Need at least two borough values for partition demo; skipping.")
        return

    target = str(boroughs[0])
    os.makedirs(demo_root, exist_ok=True)
    mono = os.path.join(demo_root, "monolithic.parquet")
    part_root = os.path.join(demo_root, "partitioned")

    if os.path.isdir(part_root):
        shutil.rmtree(part_root)
    os.makedirs(part_root, exist_ok=True)

    df.write_parquet(mono)
    parts = df.partition_by("borough", as_dict=True)
    for key, part_df in parts.items():
        b = key[0]
        safe = str(b).replace(os.sep, "_")
        out_dir = os.path.join(part_root, f"borough={safe}")
        os.makedirs(out_dir, exist_ok=True)
        # Omit borough from file payload; hive_partitioning restores it from the path.
        part_df.drop("borough").write_parquet(os.path.join(out_dir, "part.parquet"))

    # Monolithic: must read file (column pruning still helps; no directory pruning)
    t0 = time.perf_counter()
    mono_count = (
        pl.scan_parquet(mono)
        .filter(pl.col("borough") == target)
        .select(pl.len())
        .collect()
        .item()
    )
    t_mono = time.perf_counter() - t0

    # Partitioned: Polars can skip entire borough=* directories for hive layouts
    glob_pat = os.path.join(part_root, "**", "*.parquet")
    t0 = time.perf_counter()
    part_count = (
        pl.scan_parquet(glob_pat, hive_partitioning=True)
        .filter(pl.col("borough") == target)
        .select(pl.len())
        .collect()
        .item()
    )
    t_part = time.perf_counter() - t0

    logger.info("=== Polars: partition pruning (borough=*) ===")
    logger.info("Filter borough == %r (monolithic count == partitioned: %s)", target, mono_count == part_count)
    logger.info("Monolithic scan+filter wall time: %.4fs (rows=%s)", t_mono, mono_count)
    logger.info("Partitioned scan+filter wall time: %.4fs (rows=%s)", t_part, part_count)
    if t_part > 0:
        logger.info("Speedup monolithic/partitioned: %.2fx", t_mono / t_part)


def run_all(db: Database, df: pl.DataFrame, output_dir: str) -> None:
    run_mongodb_index_benchmark(db)
    demo_root = os.path.join(output_dir, "query_modeling_demo")
    run_partition_pruning_demo(df, demo_root)
