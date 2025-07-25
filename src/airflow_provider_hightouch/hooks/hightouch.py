"""Hightouch Hook for Airflow."""

from __future__ import annotations

import datetime
import time
from typing import Any
from urllib.parse import urljoin

from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpAsyncHook, HttpHook
from asgiref.sync import sync_to_async

from airflow_provider_hightouch import __version__, utils
from airflow_provider_hightouch.consts import (
    DEFAULT_POLL_INTERVAL,
    HIGHTOUCH_API_BASE_V3,
    PENDING_STATUSES,
    SUCCESS,
    TERMINAL_STATUSES,
    WARNING,
)
from airflow_provider_hightouch.types import HightouchOutput


class HightouchHook(HttpHook):
    """
    Hightouch API Hook.

    Args:
        hightouch_conn_id (str):  The name of the Airflow connection
        with connection information for the Hightouch API
        api_version: (optional(str)). Hightouch API version.
    """

    def __init__(
        self,
        *,
        hightouch_conn_id: str = "hightouch_default",
        api_version: str = "v3",
        request_max_retries: int = 3,
        request_retry_delay: float = 0.5,
        sync_id: str | None = None,
        sync_slug: str | None = None,
    ):
        if not sync_id and not sync_slug:
            raise AirflowException("One of sync_id or sync_slug must be provided to trigger a sync.")

        if api_version not in ("v1", "v3"):
            raise AirflowException("This version of the Hightouch Operator only supports the v1/v3 API.")

        super().__init__(http_conn_id=hightouch_conn_id)

        self.hightouch_conn_id = hightouch_conn_id
        self.api_version = api_version
        self._request_max_retries = request_max_retries
        self._request_retry_delay = request_retry_delay
        self.user_agent = "AirflowHightouchOperator/" + __version__

    @property
    def api_base_url(self) -> str:
        """Returns the correct API BASE URL depending on the API version."""
        return HIGHTOUCH_API_BASE_V3

    def make_request(
        self,
        method: str,
        endpoint: str,
        data: dict[str, Any] | None = None,
    ):
        """
        Create and send a request to the desired Hightouch API endpoint.

        Args:
            method: The http method use for this request (e.g. "GET", "POST").
            endpoint: The Hightouch API endpoint to send this request to.
            params: Query parameters to pass to the API endpoint
            body: Body parameters to pass to the API endpoint
        Returns:
            dict[str, Any]: Parsed json data from the response to this request
        """
        conn = self.get_connection(self.hightouch_conn_id)
        token = conn.password

        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {token}",
            "User-Agent": self.user_agent,
        }

        num_retries = 0
        while True:
            try:
                self.method = method
                response = self.run(
                    endpoint=urljoin(self.api_base_url, endpoint),
                    data=data,
                    headers=headers,
                )
                resp_dict = response.json()
                return resp_dict.get("data", resp_dict)
            except AirflowException as e:
                self.log.error("Request to Hightouch API failed: %s", e)
                if num_retries == self._request_max_retries:
                    break
                num_retries += 1
                time.sleep(self._request_retry_delay)

        raise AirflowException("Exceeded max number of retries.")

    def get_sync_run_details(self, sync_id: str, sync_request_id: str) -> list[dict[str, Any]]:
        """
        Get details about a given sync run from the Hightouch API.

        Args:
            sync_id (str): The Hightouch Sync ID.
            sync_request_id (str): The Hightouch Sync Request ID.
        Returns:
            Dict[str, Any]: Parsed json data from the response
        """
        params = {"runId": sync_request_id}

        return self.make_request(method="GET", endpoint=f"syncs/{sync_id}/runs", data=params)

    def get_sync_details(self, sync_id: str) -> dict[str, Any]:
        """
        Get details about a given sync from the Hightouch API.

        Args:
            sync_id (str): The Hightouch Sync ID.
        Returns:
            Dict[str, Any]: Parsed json data from the response.
        """
        return self.make_request(method="GET", endpoint=f"syncs/{sync_id}")

    def get_sync_from_slug(self, sync_slug: str) -> str:
        """
        Get details about a given sync from the Hightouch API.

        Args:
            sync_id (str): The Hightouch Sync ID.
        Returns:
            Dict[str, Any]: Parsed json data from the response.
        """
        r = self.make_request(method="GET", endpoint="syncs", data={"slug": sync_slug})

        if not r or not isinstance(r, list):
            raise AirflowException(f"Sync with slug {sync_slug} not found.")

        return r[0].get("id", None)

    def start_sync(self, sync_id: str, sync_slug: str) -> str:
        """
        Trigger a sync and initiate a sync run.

        Args:
            sync_id (str): The Hightouch Sync ID.
            sync_slug (str): The Hightouch Sync Slug.
        Returns:
            str: The sync request ID created by the Hightouch API.
        """
        return self.make_request(
            method="POST",
            endpoint="syncs/trigger",
            data={"syncId": sync_id, "syncSlug": sync_slug},
        )["id"]

    def poll_sync(
        self,
        sync_id: str,
        sync_request_id: str,
        fail_on_warning: bool = False,
        poll_interval: float = DEFAULT_POLL_INTERVAL,
        poll_timeout: float | None = None,
    ) -> HightouchOutput:
        """
        Poll for the completion of a sync.

        Args:
            sync_id (str): The Hightouch Sync ID
            sync_request_id (str): The Hightouch Sync Request ID to poll against.
            fail_on_warning (bool): Whether a warning is considered a failure for this sync.
            poll_interval (float): The time in seconds that will be waited between succcessive polls
            poll_timeout (float): The maximum time that will be waited before this operation
                times out.
        Returns:
            Dict[str, Any]: Parsed json output from the API.
        """
        poll_start = datetime.datetime.now()
        while True:
            sync_run_details = self.get_sync_run_details(sync_id=sync_id, sync_request_id=sync_request_id)[0]

            self.log.debug(sync_run_details)
            run = utils.parse_sync_run_details(sync_run_details)
            self.log.info(
                "Polling Hightouch Sync %s. Current status: %s. %i \\% completed.",
                sync_id,
                run.status,
                100 * run.completion_ratio,
            )

            if run.status in TERMINAL_STATUSES:
                self.log.info("Sync request status: %s. Polling complete", run.status)
                if run.error:
                    self.log.info("Sync Request Error: %s", run.error)

                if run.status == SUCCESS:
                    break
                if run.status == WARNING and not fail_on_warning:
                    break
                raise AirflowException(
                    f"Sync {sync_id} for request: {sync_request_id} failed with status: "
                    f"{run.status} and error:  {run.error}",
                )
            if run.status not in PENDING_STATUSES:
                self.log.warning(
                    "Unexpected status: %s returned for sync %s and request %s. Will try "
                    "again, but if you see this error, please let someone at Hightouch know.",
                    run.status,
                    sync_id,
                    sync_request_id,
                )
            if poll_timeout and datetime.datetime.now() > poll_start + datetime.timedelta(
                seconds=poll_timeout
            ):
                raise AirflowException(
                    f"Sync {sync_id} for request: {sync_request_id}' time out after "
                    f"{datetime.datetime.now() - poll_start}. Last status was {run.status}."
                )

            time.sleep(poll_interval)
        sync_details = self.get_sync_details(sync_id)

        return HightouchOutput(sync_details, sync_run_details)


class HightouchAsyncHook(HttpAsyncHook):
    """
    Asynchronous hook to interact with the Hightouch API.

    :param hightouch_conn_id: Connection ID for Hightouch, defaults to "hightouch_default".
    :type hightouch_conn_id: str
    :param api_version: API version to use, defaults to "v3". Supported versions are "v1" and "v3".
    :type api_version: str
    :param request_max_retries: Maximum number of retries for a request, defaults to 3.
    :type request_max_retries: int
    :param request_retry_delay: Delay between retries in seconds, defaults to 0.5.
    :type request_retry_delay: float

    :raises AirflowException: If the provided API version is not supported.
    """

    def __init__(
        self,
        hightouch_conn_id: str = "hightouch_default",
        api_version: str = "v3",
        request_max_retries: int = 3,
        request_retry_delay: float = 0.5,
        **kwargs,
    ):
        self.hightouch_conn_id = hightouch_conn_id
        self.api_version = api_version
        self._request_max_retries = request_max_retries
        self._request_retry_delay = request_retry_delay
        if self.api_version not in ("v1", "v3"):
            raise AirflowException("This version of the Hightouch Operator only supports the v1/v3 API.")
        self.user_agent = "AirflowHightouchAsyncTrigger/" + __version__

        super().__init__(
            http_conn_id=hightouch_conn_id,
            retry_delay=request_retry_delay,
            retry_limit=request_max_retries,
            **kwargs,
        )

    async def get_headers(self) -> dict[str, str]:
        connection = await sync_to_async(self.get_connection(self.hightouch_conn_id))
        headers = {
            "Authorization": f"Bearer {connection.password}",
            "Content-Type": "application/json",
            "User-Agent": self.user_agent,
        }

        return headers
