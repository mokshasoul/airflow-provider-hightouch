"""
This module contains utility functions for the Hightouch provider.
"""

from typing import Type

from dateutil import parser

from airflow_provider_hightouch.types import SyncRunParsedOutput


def parse_sync_run_details(sync_run_details) -> Type[SyncRunParsedOutput]:
    """
    Parses the sync run details and returns an instance of SyncRunParsedOutput.

    Args:
        sync_run_details (dict): The sync run details to be parsed.

    Returns:
        SyncRunParsedOutput: An instance of
            SyncRunParsedOutput containing the parsed details.

    """
    x = SyncRunParsedOutput()

    x.created_at = None
    x.started_at = None
    x.finished_at = None
    x.id = sync_run_details.get("id")

    if sync_run_details.get("createdAt"):
        x.created_at = parser.parse(sync_run_details["createdAt"])
    if sync_run_details.get("startedAt"):
        x.started_at = parser.parse(sync_run_details["startedAt"])
    if sync_run_details.get("finishedAt"):
        x.finished_at = parser.parse(sync_run_details["finishedAt"])

    if x.finished_at and x.started_at:
        x.elapsed_seconds = (x.finished_at - x.started_at).seconds

    x.planned_add = sync_run_details["plannedRows"].get("addedCount")
    x.planned_change = sync_run_details["plannedRows"].get("changedCount")
    x.planned_remove = sync_run_details["plannedRows"].get("removedCount")

    x.successful_add = sync_run_details["successfulRows"].get("addedCount")
    x.successful_change = sync_run_details["successfulRows"].get("changedCount")
    x.successful_remove = sync_run_details["successfulRows"].get("removedCount")

    x.failed_add = sync_run_details["failedRows"].get("addedCount")
    x.failed_change = sync_run_details["failedRows"].get("changedCount")
    x.failed_remove = sync_run_details["failedRows"].get("removedCount")

    x.query_size = sync_run_details.get("querySize")
    x.status = sync_run_details.get("status")
    x.completion_ratio = float(sync_run_details.get("completionRatio", 0))
    x.error = sync_run_details.get("error")

    return x


def generate_metadata_from_parsed_run(parsed_output: SyncRunParsedOutput):
    """
    Generate metadata from the parsed run.

    Args:
        parsed_output (SyncRunParsedOutput): The parsed output of the run.

    Returns:
        dict: A dictionary containing the generated metadata.

    """
    return {
        "elapsed_seconds": parsed_output.elapsed_seconds or 0,
        "planned_add": parsed_output.planned_add,
        "planned_change": parsed_output.planned_change,
        "planned_remove": parsed_output.planned_remove,
        "successful_add": parsed_output.successful_add,
        "successful_change": parsed_output.successful_change,
        "successful_remove": parsed_output.successful_remove,
        "failed_add": parsed_output.failed_add,
        "failed_change": parsed_output.failed_change,
        "failed_remove": parsed_output.failed_remove,
        "query_size": parsed_output.query_size,
    }
