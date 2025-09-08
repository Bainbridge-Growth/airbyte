import json
import os
from datetime import datetime, timezone
from unittest.mock import MagicMock

import freezegun
import pytest
from source_quickbooks_drivepoint.source import SourceQuickbooksDrivepoint

_CONFIG = {
    "realm_id": "123456789",
    "start_date": "2024-01-01",
    "end_date": "2024-12-31",
    "credentials": {
        "client_id": "test_client_id",
        "client_secret": "test_client_secret",
        "refresh_token": "test_refresh_token"
    }
}

_NOW = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)


# Load test data from JSON file
def load_test_data(filename):
    with open(os.path.join(os.path.dirname(__file__), "resources/", filename)) as fp:
        return json.load(fp)

def source_full_refresh_and_compare(requests_mock, test_data_file_name, expected_output_data_size, num_of_rows_to_check = 1):
    """Test reading a complete balance sheet report"""

    # Mock the OAuth token refresh endpoint
    requests_mock.post(
        "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
        json={"access_token": "fake-token", "expires_in": 3600, "token_type": "Bearer"}
    )

    # Mock the balance sheet API call
    requests_mock.get(
        "https://quickbooks.api.intuit.com/v3/company/123456789/reports/BalanceSheet",
        json=load_test_data("api_responses/%s" % test_data_file_name)
    )

    source = SourceQuickbooksDrivepoint()

    # Test that streams can be created
    streams = source.streams(_CONFIG)
    assert len(streams) > 0

    # Find the balance sheet stream
    balance_sheet_stream = None
    # TODO: un-hardcode BalanceSheet from here ?
    for stream in streams:
        if hasattr(stream, '__class__') and 'BalanceSheet' in stream.__class__.__name__:
            balance_sheet_stream = stream
            break

    assert balance_sheet_stream is not None, "BalanceSheetReportStream not found in streams for %s" % test_data_file_name

    # Test reading records from the stream
    records = list(balance_sheet_stream.read_records(sync_mode="full_refresh"))

    # TODO: instead of using hardcoded expected_output_data_size number, use the len of expected json data file
    assert len(records) == expected_output_data_size

    # Load expected results and compare with first record
    expected_results = load_test_data("expected_results/%s" % test_data_file_name)

    for idx, value in enumerate(expected_results):
        if idx >= num_of_rows_to_check:
            break

        compare_records(expected_results[idx], records[idx], idx)

def compare_records(expected_record, actual_record, index = 0):
    for key, expected_value in expected_record.items():
        assert key in actual_record, f"Missing key '{key}' in actual result at index {index}"
        actual_value = actual_record[key]
        assert actual_value == expected_value, f"Key '{key}': expected '{expected_value}', got '{actual_value} at index {index}'"

@freezegun.freeze_time(_NOW.isoformat())
def test_balance_sheet_simple(requests_mock):
    source_full_refresh_and_compare(requests_mock, "balance_sheet_simple.json", 15, 2)

@freezegun.freeze_time(_NOW.isoformat())
def test_balance_sheet_nguyen_without_classes_20240423(requests_mock):
    source_full_refresh_and_compare(requests_mock, "balance_sheet_nguyen_without_classes_20240423.json", 117, 1)

@freezegun.freeze_time(_NOW.isoformat())
def test_balance_sheet_nguyen_with_classes_20240423(requests_mock):
    source_full_refresh_and_compare(requests_mock, "balance_sheet_nguyen_with_classes_20240423.json", 833, 14)

@freezegun.freeze_time(_NOW.isoformat())
def test_balance_sheet_dirtylabs_11_levels_deep(requests_mock):
    source_full_refresh_and_compare(requests_mock, "balance_sheet_dirtylabs_11_levels_deep.json", 68, 5)
