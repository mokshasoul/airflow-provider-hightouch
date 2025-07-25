"""Airflow Trigger for HighTouch Syncs."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

from airflow.exceptions import AirflowException
from airflow.providers.http.triggers.http import HttpTrigger
from airflow.triggers.base import TriggerEvent

from airflow_provider_hightouch.consts import FAILED, TERMINAL_STATUSES
from airflow_provider_hightouch.hooks.hightouch import HightouchAsyncHook


class HightouchSyncTrigger(HttpTrigger):
    """Trigger to handle HighTouch syncs in Airflow."""

    def __init__(
        self,
        *,
        sync_id: str | None = None,
        hightouch_conn_id: str = "hightouch_default",
        api_version: str = "v3",
        timeout: int = 3600,
        sync_request_id: str = None,
        poll_interval: float = 30.0,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.hightouch_conn_id = hightouch_conn_id
        self.api_version = api_version
        self.sync_id = sync_id
        self.sync_request_id = sync_request_id
        self.timeout = timeout
        self.headers = {"Content-Type": "application/json"}
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize HighTouchTrigger arguments and classpath."""
        return (
            "airflow_provider_hightouch.triggers.HightouchSyncTrigger",
            {
                "hightouch_conn_id": self.hightouch_conn_id,
                "api_version": self.api_version,
                "sync_id": self.sync_id,
                "sync_request_id": self.sync_request_id,
                "timeout": self.timeout,
                "poll_interval": self.poll_interval,
                "method": self.method,
                "auth_type": self.auth_type,
                "endpoint": self.endpoint,
                "headers": self.headers,
                "data": self.data,
                "extra_options": self.extra_options,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        try:
            # Trigger the sync
            if not self.sync_request_id:
                trigger_response = await self.trigger_sync()
                if trigger_response.get("status_code", 404) != 200:
                    yield TriggerEvent(
                        {"status": "error", "message": "Failed to trigger HighTouch sync {self.sync_id}"}
                    )
                self.sync_request_id = trigger_response.get("id")

            self.log.info("Polling sync %s with request id %s", self.sync_id, self.sync_request_id)
            #
            # Check the sync status periodically
            start_time = asyncio.get_event_loop().time()
            while True:
                if asyncio.get_event_loop().time() - start_time > self.timeout:
                    yield TriggerEvent({"status": "timeout"})

                status_response = self.check_sync_status()
                if status_response.get("status", FAILED) in TERMINAL_STATUSES:
                    yield TriggerEvent({"status": "completed", "status_response": status_response})

                await asyncio.sleep(self.poll_interval)

        except (AirflowException, KeyError) as e:
            yield TriggerEvent({"status": "error", "message": str(e)})

    async def trigger_sync(self) -> dict[str, Any]:
        hook = HightouchAsyncHook(http_conn_id=self.hightouch_conn_id, method="POST")
        payload = {"syncId": self.sync_id, "syncSlug": self.sync_slug}
        url = "syncs/trigger"
        headers = hook.get_headers()
        response = hook.run(endpoint=url, json=payload, headers=headers)
        parsed_response = await self._convert_response(response)

        return parsed_response.json().get("data", {})

    async def check_sync_status(self) -> dict[str, Any]:
        hook = HightouchAsyncHook(http_conn_id=self.hightouch_conn_id, method="GET")
        url = f"{self.api_version}/syncs/{self.sync_id}/status"
        headers = hook.get_headers()
        payload = {"runId": self.sync_request_id}
        response = hook.run(endpoint=url, json=payload, headers=headers)
        parsed_response = await HttpTrigger._convert_response(response)

        return parsed_response.json().get("data", {})
