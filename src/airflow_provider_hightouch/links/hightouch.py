"""
Link module for Hightouch Operators
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.models.baseoperator import BaseOperatorLink
from airflow.plugins_manager import AirflowPlugin
from airflow_provider_hightouch.operators.hightouch import HightouchTriggerSyncOperator

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstanceKey


class HightouchLink(BaseOperatorLink):
    name = "Hightouch Sync"
    operators = [HightouchTriggerSyncOperator]

    def get_link(self, operator: HightouchTriggerSyncOperator, *, ti_key: TaskInstanceKey) -> str:
        sync_id = operator.sync_id
        return f"https://app.hightouch.io/{sync_id}"


class HighTouchExtraLinkPlugin(AirflowPlugin):
    name = "hightouch_extra_link_plugin"
    operator_extra_links = [HightouchLink()]
