"""Link module for Hightouch Operators."""

from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.models.baseoperator import BaseOperatorLink
from airflow.plugins_manager import AirflowPlugin

from airflow_provider_hightouch.operators.hightouch import HightouchTriggerSyncOperator

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstanceKey


class HightouchLink(BaseOperatorLink):
    """
    HightouchLink is a custom Airflow operator link that generates a URL to the Hightouch sync page.

    Attributes:
        name (str): The name of the link, which is "Hightouch Sync".
        operators (list): A list of operators that this link is associated with, which includes HightouchTriggerSyncOperator.

    Methods:
        get_link(operator: HightouchTriggerSyncOperator, *, ti_key: TaskInstanceKey) -> str:
            Generates a URL to the Hightouch sync page using the sync_id from the provided operator.

    Args:
        operator (HightouchTriggerSyncOperator): The operator instance from which to extract the sync_id.
        ti_key (TaskInstanceKey): The task instance key (not used in the method but required by the interface).

    Returns:
        str: The URL to the Hightouch sync page.
    """

    name = "Hightouch Sync"
    operators = [HightouchTriggerSyncOperator]

    def get_link(self, operator: HightouchTriggerSyncOperator, *, ti_key: TaskInstanceKey) -> str:
        sync_id = operator.sync_id
        return f"https://app.hightouch.io/{sync_id}"


class HighTouchExtraLinkPlugin(AirflowPlugin):
    """
    HighTouchExtraLinkPlugin is an Airflow plugin that adds extra links to Airflow operators.

    Attributes:
        name (str): The name of the plugin.
        operator_extra_links (list): A list of extra links to be added to Airflow operators.
    """

    name = "hightouch_extra_link_plugin"
    operator_extra_links = [HightouchLink()]
