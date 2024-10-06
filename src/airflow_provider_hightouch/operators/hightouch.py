"""Hightouch Operator to execute a sync run."""

from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator

from airflow_provider_hightouch.hooks.hightouch import HightouchHook
from airflow_provider_hightouch.triggers.hightouch import HightouchSyncTrigger
from airflow_provider_hightouch.utils import parse_sync_run_details

if TYPE_CHECKING:
    from airflow.models import Context


class HightouchTriggerSyncOperator(BaseOperator):
    """
    Triggers run for a specified Sync in Hightouch via the Hightouch API.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`https://hightouch.io/docs/integrations/airflow/`

    :param sync_id: ID of the sync to trigger
    :type sync_id: int
    :param sync_slug: Slug of the sync to trigger
    :param connection_id: Name of the connection to use, defaults to hightouch_default
    :type connection_id: str
    :param api_version: Hightouch API version. Only v3 is supported.
    :type api_version: str
    :param synchronous: Whether to wait for the sync to complete before completing the task
    :type synchronous: bool
    :param error_on_warning: Should sync warnings be treated as errors or ignored?
    :type error_on_warning: bool
    :param wait_seconds: Time to wait in between subsequent polls to the API.
    :type wait_seconds: float
    :param timeout: Maximum time to wait for a sync to complete before aborting
    :type timeout: int
    """

    def __init__(
        self,
        *,
        sync_id: str | None = None,
        sync_slug: str | None = None,
        connection_id: str = "hightouch_default",
        api_version: str = "v3",
        synchronous: bool = True,
        error_on_warning: bool = False,
        wait_seconds: float = 3,
        timeout: int = 3600,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.hightouch_conn_id = connection_id
        self.api_version = api_version
        if not sync_id and not sync_slug:
            raise ValueError("One of sync_id or sync_slug must be provided to trigger a sync")
        self.sync_id = sync_id
        self.sync_slug = sync_slug
        self.error_on_warning = error_on_warning
        self.synchronous = synchronous
        self.wait_seconds = wait_seconds
        self.timeout = timeout
        self.deferrable = deferrable

    def execute(self, context) -> str:
        """Start a Hightouch Sync Run."""
        hook = HightouchHook(
            hightouch_conn_id=self.hightouch_conn_id,
            api_version=self.api_version,
            sync_id=self.sync_id,
            sync_slug=self.sync_slug,
        )
        sync = self.sync_id or self.sync_slug
        self.sync_id = self.sync_id or hook.get_sync_from_slug(self.sync_slug)

        request_id = hook.start_sync(self.sync_id, self.sync_slug)

        self.log.info("Successfully created request %s to start sync: %s", request_id, sync)

        sync_run_details = hook.get_sync_run_details(sync_id=self.sync_id, sync_request_id=request_id)

        if not self.synchronous:
            return sync_run_details

        if not self.deferrable:
            self.log.info("Start synchronous request to run a sync.")
            hightouch_output = hook.poll_sync(
                sync_id=self.sync_id,
                sync_request_id=request_id,
                fail_on_warning=self.error_on_warning,
                poll_interval=self.wait_seconds,
                poll_timeout=self.timeout,
            )

            try:
                parsed_result = parse_sync_run_details(sync_run_details=hightouch_output.sync_run_details)
                self.log.debug("%s", dict(parsed_result))
                self.log.info("Sync completed successfully")
                return parsed_result.id
            except Exception:
                self.log.exception("Sync ran successfully but failed to parse output.")
                self.log.exception(hightouch_output)
                return None

        self.defer(
            trigger=HightouchSyncTrigger(
                hightouch_conn_id=self.hightouch_conn_id,
                api_version=self.api_version,
                sync_id=self.sync_id,
                sync_request_id=request_id,
                timeout=self.timeout,
                poll_interval=self.wait_seconds,
            ),
            timeout=self.timeout,
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict[str, any]) -> str:
        if event["status"] == "completed":
            return event.get("message", "No message")

        raise AirflowException(
            f"Sync run failed with status {event['status']}: {event.get('message', 'No message')}"
        )
