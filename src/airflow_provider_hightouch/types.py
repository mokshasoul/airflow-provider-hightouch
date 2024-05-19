"""Custom types for the Hightouch provider."""

import datetime as dt
from typing import Any, Dict, NamedTuple


class HightouchOutput(
    NamedTuple(
        "_HightouchOutput",
        [
            ("sync_details", Dict[str, Any]),
            ("sync_run_details", Dict[str, Any]),
        ],
    )
):
    """
    Contains recorded information about the state of a Hightouch sync after a sync completes.
    Attributes:
        sync_details (Dict[str, Any]):
            https://hightouch.io/docs/api-reference/#operation/GetSync
        sync_run_details (Dict[str, Any]):
            https://hightouch.io/docs/api-reference/#operation/ListSyncRuns
        destination_details (Dict[str, Any]):
            https://hightouch.io/docs/api-reference/#operation/GetDestination
    """


class SyncRunParsedOutput(
    NamedTuple(
        "_SyncRunParsedOutput",
        [
            ("id", int),
            ("created_at", dt.datetime),
            ("started_at", dt.datetime),
            ("finished_at", dt.datetime),
            ("elapsed_seconds", int),
            ("planned_add", int),
            ("planned_change", int),
            ("planned_remove", int),
            ("successful_add", int),
            ("successful_change", int),
            ("successful_remove", int),
            ("failed_add", int),
            ("failed_change", int),
            ("failed_remove", int),
            ("query_size", int),
            ("status", str),
            ("completion_ratio", str),
            ("error", str),
        ],
    )
):
    """
    Contains parsed information about a Hightouch sync run.
    """
