# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

"""Live sandbox tests for the GMV Max streams.

Unlike the other tests in this `integration/` directory, these make real network
calls against TikTok's sandbox API instead of mocking HTTP responses. Everything
else in this connector's test suite is HttpMocker-based specifically to avoid
hitting TikTok at all during normal development and CI - these tests are the
deliberate exception, added to confirm the gmv_max_* streams work against a real
account, since GMV Max data (a TikTok Shop with GMV Max campaigns configured)
can't be faithfully faked with mocked responses.

Requirements to run:
  1. A sandbox app with a TikTok Shop that has GMV Max Campaigns configured.
     A generic/empty sandbox account will not have gmv_max_stores or
     gmv_max_campaigns data and these tests will fail on the "returns records"
     assertions - that is expected, not a bug in the connector.
  2. `secrets/sandbox_config.json` (relative to the connector root) with:
       {
         "credentials": {
           "auth_type": "sandbox_access_token",
           "advertiser_id": "<sandbox advertiser id>",
           "access_token": "<sandbox access token>"
         }
       }
     `start_date` is optional; if omitted, these tests default it to 30 days
     ago to keep each run to a handful of requests.

Run explicitly with:

    poetry run pytest -m sandbox_live unit_tests/integration/test_gmv_max_sandbox_live.py

These are excluded from the default test run (see the `addopts` in
unit_tests/pyproject.toml) even when the secrets file is present. TikTok's
sandbox has a 10 req/s rate limit that, if exceeded (e.g. by running these
concurrently with CI or another local run against the same credentials), can
lock the credentials out for hours - see AGENTS.md #6. Never run these at the
same time as anything else using the same sandbox credentials.
"""

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict

import pytest

from airbyte_cdk.models import SyncMode
from airbyte_cdk.test.catalog_builder import CatalogBuilder
from airbyte_cdk.test.entrypoint_wrapper import EntrypointOutput, read

from ..conftest import get_source


pytestmark = pytest.mark.sandbox_live

SECRETS_PATH = Path(__file__).parent.parent.parent / "secrets" / "sandbox_config.json"

if not SECRETS_PATH.exists():
    pytest.skip(
        f"{SECRETS_PATH} not found - add sandbox credentials there to run these tests (see module docstring).",
        allow_module_level=True,
    )


def _sandbox_config() -> Dict[str, Any]:
    config = json.loads(SECRETS_PATH.read_text())
    config.setdefault("start_date", (date.today() - timedelta(days=30)).isoformat())
    return config


def _read(stream_name: str) -> EntrypointOutput:
    config = _sandbox_config()
    catalog = CatalogBuilder().with_stream(name=stream_name, sync_mode=SyncMode.full_refresh).build()
    return read(get_source(config=config, state=None), config, catalog)


class TestGmvMaxStoresSandbox:
    def test_read_returns_records(self):
        output = _read("gmv_max_stores")
        assert not output.errors, [e.trace.error.message for e in output.errors]
        assert len(output.records) > 0, "No gmv_max_stores records - does this sandbox account have a TikTok Shop configured?"
        record = output.records[0].record.data
        assert record["store_id"]
        assert record["is_gmv_max_available"] is True


class TestGmvMaxCampaignsSandbox:
    def test_read_returns_records(self):
        output = _read("gmv_max_campaigns")
        assert not output.errors, [e.trace.error.message for e in output.errors]
        assert len(output.records) > 0, "No gmv_max_campaigns records - does this sandbox account have GMV Max Campaigns configured?"
        record = output.records[0].record.data
        assert record["campaign_id"]
        assert record["advertiser_id"]


class TestGmvMaxCampaignReportsDailySandbox:
    def test_read_returns_records(self):
        output = _read("gmv_max_campaign_reports_daily")
        assert not output.errors, [e.trace.error.message for e in output.errors]
        assert len(output.records) > 0, (
            "No gmv_max_campaign_reports_daily records - this requires both a GMV Max "
            "Campaign and delivery/spend in the last 30 days."
        )
        record = output.records[0].record.data
        assert record["advertiser_id"]
        assert record["stat_time_day"]
        assert "cost" in record["metrics"]
