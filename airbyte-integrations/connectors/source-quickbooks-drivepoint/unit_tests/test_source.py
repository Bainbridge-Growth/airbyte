import json
import os
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import freezegun
import pytest
from source_quickbooks_drivepoint.source import SourceQuickbooksDrivepoint
from source_quickbooks_drivepoint.report_streams import RESULT_SET_BIG_ERROR_CODE

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

@pytest.fixture
def mock_firebase_client():
    """Mock Firebase client to avoid external dependencies in tests"""
    with patch('source_quickbooks_drivepoint.auth_client.FirebaseClient') as mock_fb, \
         patch('source_quickbooks_drivepoint.auth_client.SecretManagerClient') as mock_sm, \
         patch('source_quickbooks_drivepoint.auth_client.os.path.exists') as mock_exists:

        # Make os.path.exists return False so it uses SecretManagerClient path
        mock_exists.return_value = False

        # Mock SecretManagerClient
        mock_sm_instance = MagicMock()
        mock_sm_instance.get_firebase_service_account.return_value = {}
        mock_sm.return_value = mock_sm_instance

        # Mock FirebaseClient
        mock_fb_instance = MagicMock()
        mock_fb_instance.get_realm_id.return_value = "123456789"
        mock_fb_instance.get_refresh_token.return_value = "test_refresh_token"
        mock_fb.return_value = mock_fb_instance

        yield mock_fb_instance

def source_full_refresh_and_compare(report_type, requests_mock, mock_firebase, test_data_file_name, expected_output_data_size, num_of_rows_to_check = 1):
    """Test reading a complete balance sheet report"""

    requests_mock.post(
        "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
        json={"access_token": "fake-token", "expires_in": 3600, "token_type": "Bearer"}
    )

    requests_mock.get(
        "https://quickbooks.api.intuit.com/v3/company/123456789/reports/BalanceSheet",
        json=load_test_data("api_responses/%s" % test_data_file_name)
    )

    requests_mock.get(
        "https://quickbooks.api.intuit.com/v3/company/123456789/reports/ProfitAndLoss",
        json=load_test_data("api_responses/%s" % test_data_file_name)
    )

    source = SourceQuickbooksDrivepoint()

    # Test that streams can be created
    streams = source.streams(_CONFIG)
    assert len(streams) > 0

    # Find the balance sheet stream
    report_stream = None
    for stream in streams:
        if hasattr(stream, '__class__') and report_type in stream.__class__.__name__:
            report_stream = stream
            break

    assert report_stream is not None, "Report stream not found in streams for %s report" % report_type

    # Test reading records from the stream
    records = list(report_stream.read_records(sync_mode="full_refresh"))

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
def test_balance_sheet_simple(requests_mock, mock_firebase_client):
    source_full_refresh_and_compare("BalanceSheet", requests_mock, mock_firebase_client, "balance_sheet_simple.json", 16, 2)

@freezegun.freeze_time(_NOW.isoformat())
def test_balance_sheet_nguyen_without_classes_20240423(requests_mock, mock_firebase_client):
    source_full_refresh_and_compare("BalanceSheet", requests_mock, mock_firebase_client, "balance_sheet_nguyen_without_classes_20240423.json", 121, 1)

@freezegun.freeze_time(_NOW.isoformat())
def test_balance_sheet_nguyen_with_classes_20240423(requests_mock, mock_firebase_client):
    source_full_refresh_and_compare("BalanceSheet", requests_mock, mock_firebase_client, "balance_sheet_nguyen_with_classes_20240423.json", 861, 14)

@freezegun.freeze_time(_NOW.isoformat())
def test_balance_sheet_dirtylabs_11_levels_deep(requests_mock, mock_firebase_client):
    source_full_refresh_and_compare("BalanceSheet", requests_mock, mock_firebase_client, "balance_sheet_dirtylabs_11_levels_deep.json", 90, 5)

@freezegun.freeze_time(_NOW.isoformat())
def test_pandl_nguyen_with_classes_20240423(requests_mock, mock_firebase_client):
    source_full_refresh_and_compare("ProfitLoss", requests_mock, mock_firebase_client, "pandl_nguyen_with_classes_20240423.json", 126, 5)

def _run_transaction_list_test(requests_mock, fixture_name):
    requests_mock.post(
        "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
        json={"access_token": "fake-token", "expires_in": 3600, "token_type": "Bearer"}
    )
    requests_mock.get(
        "https://quickbooks.api.intuit.com/v3/company/123456789/reports/TransactionList",
        json=load_test_data(f"api_responses/{fixture_name}")
    )

    source = SourceQuickbooksDrivepoint()
    streams = source.streams(_CONFIG)
    report_stream = next(s for s in streams if "TransactionList" in s.__class__.__name__)

    records = list(report_stream.read_records(sync_mode="full_refresh"))

    expected_results = load_test_data(f"expected_results/{fixture_name}")
    assert len(records) == len(expected_results)
    for idx, expected in enumerate(expected_results):
        compare_records(expected, records[idx], idx)

@freezegun.freeze_time(_NOW.isoformat())
def test_transaction_list_simple(requests_mock, mock_firebase_client):
    """TransactionList emits one record per Data row (ignores dimensions; yearly slicing
    means one API call for the configured 2024 date range)."""
    _run_transaction_list_test(requests_mock, "transaction_list_simple.json")

@freezegun.freeze_time(_NOW.isoformat())
def test_transaction_list_subt_nat_amount(requests_mock, mock_firebase_client):
    """Production responses use ColType 'subt_nat_amount' (transaction currency) instead of
    'subt_nat_home_amount' (home currency, which the QBO docs sample uses). Both must map to Amount."""
    _run_transaction_list_test(requests_mock, "transaction_list_subt_nat_amount.json")

@freezegun.freeze_time(_NOW.isoformat())
def test_balance_sheet_with_departments_second_dimension(requests_mock, mock_firebase_client):
    """Test BalanceSheet report with first_dimension (Classes) and second_dimension (Departments)"""

    # Mock the OAuth token refresh endpoint
    requests_mock.post(
        "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
        json={"access_token": "fake-token", "expires_in": 3600, "token_type": "Bearer"}
    )

    # Mock the Departments query endpoint (second_dimension)
    query_mock = requests_mock.get(
        "https://quickbooks.api.intuit.com/v3/company/123456789/query",
        json=load_test_data("api_responses/departments_query.json")
    )

    # Mock the BalanceSheet API calls for each department and month
    # Format: (department_id, department_name, start_date, end_date)
    # Note: department_id=None means this is the TOTAL call (no department filter)
    test_scenarios = [
        (None, "TOTAL", "2024-01-01", "2024-01-31"),  # Jan TOTAL
        (1, "Sales", "2024-01-01", "2024-01-31"),
        (2, "Marketing", "2024-01-01", "2024-01-31"),
        (None, "TOTAL", "2024-02-01", "2024-02-28"),  # Feb TOTAL
        (1, "Sales", "2024-02-01", "2024-02-28"),
        (2, "Marketing", "2024-02-01", "2024-02-28"),
    ]

    report_mocks = []
    for dept_id, dept_name, start_date, end_date in test_scenarios:
        # Determine the month for the file name
        month = "01" if "01-01" in start_date else "02"

        # Build the URL - if dept_id is None, don't add department parameter
        if dept_id is None:
            url = f"https://quickbooks.api.intuit.com/v3/company/123456789/reports/BalanceSheet?accounting_method=Accrual&summarize_column_by=Classes&start_date={start_date}&end_date={end_date}"
            # Use dedicated TOTAL file that aggregates Sales + Marketing data
            file_name = f"api_responses/balance_sheet_with_departments_second_dimension_TOTAL_2024_{month}.json"
        else:
            url = f"https://quickbooks.api.intuit.com/v3/company/123456789/reports/BalanceSheet?accounting_method=Accrual&summarize_column_by=Classes&department={dept_id}&start_date={start_date}&end_date={end_date}"
            file_name = f"api_responses/balance_sheet_with_departments_second_dimension_{dept_name}_2024_{month}.json"

        mock = requests_mock.get(url, json=load_test_data(file_name))
        report_mocks.append(mock)

    # Create a special config with first_dimension and second_dimension
    config_with_second_dimension = {
        "realm_id": "123456789",
        "start_date": "2024-01-01",
        "end_date": "2024-02-28",  # Two months to simplify the test
        "credentials": {
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
            "refresh_token": "test_refresh_token"
        },
        "accounting_method": {
            "selected_method": "Accrual"
        },
        "balance_sheet_settings": {
            "summarize_column": {
                "selected_first_dimension": "Classes"
            },
            "second_dimension": {
                "selected_second_dimension": "Departments"
            }
        }
    }

    source = SourceQuickbooksDrivepoint()
    streams = source.streams(config_with_second_dimension)

    # Find the BalanceSheet stream
    balance_sheet_stream = None
    for stream in streams:
        if hasattr(stream, '__class__') and "BalanceSheet" in stream.__class__.__name__:
            balance_sheet_stream = stream
            break

    assert balance_sheet_stream is not None, "BalanceSheet stream not found"
    assert balance_sheet_stream.first_dimension == "Classes", "first_dimension should be set to Classes"
    assert balance_sheet_stream.second_dimension == "Departments", "second_dimension should be set to Departments"

    # Read records
    records = list(balance_sheet_stream.read_records(sync_mode="full_refresh"))

    # We expect records for: 2 months × (1 TOTAL + 2 departments) × 4 accounts = 24 records
    assert len(records) == 24, f"Expected 24 records (2 months × 3 dimension values × 4 accounts), got {len(records)}"

    # Load expected results
    expected_results = load_test_data("expected_results/balance_sheet_with_departments_second_dimension.json")

    # Verify first 16 records match expected results (we'll add TOTAL records to expected results separately)
    for idx in range(min(16, len(expected_results))):
        compare_records(expected_results[idx], records[idx], idx)

    # Verify each endpoint was called exactly once
    assert query_mock.call_count == 1, f"Query endpoint should be called once, was called {query_mock.call_count} times"
    for i, mock in enumerate(report_mocks):
        dept_id, dept_name, start_date, end_date = test_scenarios[i]
        label = f"TOTAL {start_date}" if dept_id is None else f"{dept_name} {start_date}"
        assert mock.call_count == 1, f"BalanceSheet endpoint for {label} should be called once, was called {mock.call_count} times"

@freezegun.freeze_time(_NOW.isoformat())
def test_pandl_with_classes_second_dimension(requests_mock, mock_firebase_client):
    """Test ProfitAndLoss report with first_dimension (Classes) and second_dimension (Departments)"""

    # Mock the OAuth token refresh endpoint
    requests_mock.post(
        "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
        json={"access_token": "fake-token", "expires_in": 3600, "token_type": "Bearer"}
    )

    # Mock the Departments query endpoint (second_dimension)
    query_mock = requests_mock.get(
        "https://quickbooks.api.intuit.com/v3/company/123456789/query",
        json=load_test_data("api_responses/departments_query.json")
    )

    # Mock the ProfitAndLoss API calls for each department
    # Format: (department_id, department_name, start_date, end_date)
    # Note: department_id=None means this is the TOTAL call (no department filter)
    test_scenarios = [
        (None, "TOTAL", "2024-01-01", "2024-01-31"),
        (1, "Sales", "2024-01-01", "2024-01-31"),
        (2, "Marketing", "2024-01-01", "2024-01-31"),
    ]

    report_mocks = []
    for dept_id, dept_name, start_date, end_date in test_scenarios:
        if dept_id is None:
            url = f"https://quickbooks.api.intuit.com/v3/company/123456789/reports/ProfitAndLoss?accounting_method=Accrual&summarize_column_by=Classes&start_date={start_date}&end_date={end_date}"
            file_name = "api_responses/pandl_with_classes_second_dimension_TOTAL.json"
        else:
            url = f"https://quickbooks.api.intuit.com/v3/company/123456789/reports/ProfitAndLoss?accounting_method=Accrual&summarize_column_by=Classes&department={dept_id}&start_date={start_date}&end_date={end_date}"
            file_name = f"api_responses/pandl_with_classes_second_dimension_{dept_name}.json"

        mock = requests_mock.get(url, json=load_test_data(file_name))
        report_mocks.append(mock)

    config_with_second_dimension = {
        "realm_id": "123456789",
        "start_date": "2024-01-01",
        "end_date": "2024-01-31",  # Single month to simplify the test
        "credentials": {
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
            "refresh_token": "test_refresh_token"
        },
        "accounting_method": {
            "selected_method": "Accrual"
        },
        "profit_loss_settings": {
            "summarize_column": {
                "selected_first_dimension": "Classes"
            },
            "second_dimension": {
                "selected_second_dimension": "Departments"
            }
        }
    }

    source = SourceQuickbooksDrivepoint()
    streams = source.streams(config_with_second_dimension)

    pandl_stream = None
    for stream in streams:
        if hasattr(stream, '__class__') and "ProfitLoss" in stream.__class__.__name__:
            pandl_stream = stream
            break

    assert pandl_stream is not None, "ProfitLoss stream not found"
    assert pandl_stream.first_dimension == "Classes", "first_dimension should be set to Classes"
    assert pandl_stream.second_dimension == "Departments", "second_dimension should be set to Departments"

    records = list(pandl_stream.read_records(sync_mode="full_refresh"))

    # Expect records for: 1 month × (1 TOTAL + 2 departments) × 5 accounts × 3 classes = 45 records
    # 5 accounts = 2 header accounts (4000, 5000) + 3 data accounts (4001, 5001, 7001)
    # 3 classes = Distribution, eCommerce, NotSpecified (TOTAL column is skipped)
    assert len(records) == 45, f"Expected 45 records (1 month × 3 dimension values × 5 accounts × 3 classes), got {len(records)}"

    expected_results = load_test_data("expected_results/pandl_with_classes_second_dimension.json")

    # Verify records match expected results
    for idx in range(min(len(expected_results), len(records))):
        compare_records(expected_results[idx], records[idx], idx)

    # Verify each endpoint was called exactly once
    assert query_mock.call_count == 1, f"Query endpoint should be called once, was called {query_mock.call_count} times"
    for i, mock in enumerate(report_mocks):
        dept_id, dept_name, start_date, end_date = test_scenarios[i]
        label = f"TOTAL {start_date}" if dept_id is None else f"{dept_name} {start_date}"
        assert mock.call_count == 1, f"ProfitAndLoss endpoint for {label} should be called once, was called {mock.call_count} times"


@freezegun.freeze_time(_NOW.isoformat())
def test_request_params_without_first_dimension(requests_mock, mock_firebase_client):
    """When neither first_dimension nor second_dimension is set the connector now
    requests summarize_column_by=Month so QBO returns one column per month inside
    a single response (yearly slicing replaces the previous monthly slicing).
    """

    requests_mock.post(
        "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
        json={"access_token": "fake-token", "expires_in": 3600, "token_type": "Bearer"}
    )

    balance_sheet_mock = requests_mock.get(
        "https://quickbooks.api.intuit.com/v3/company/123456789/reports/BalanceSheet",
        json=load_test_data("api_responses/balance_sheet_simple.json")
    )

    config_without_first_dimension = {
        "realm_id": "123456789",
        "start_date": "2024-01-01",
        "end_date": "2024-01-31",
        "credentials": {
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
            "refresh_token": "test_refresh_token"
        },
        "accounting_method": {
            "selected_method": "Accrual"
        }
    }

    source = SourceQuickbooksDrivepoint()
    streams = source.streams(config_without_first_dimension)

    balance_sheet_stream = next(
        s for s in streams if "BalanceSheet" in s.__class__.__name__
    )
    assert balance_sheet_stream.first_dimension is None

    list(balance_sheet_stream.read_records(sync_mode="full_refresh"))

    assert balance_sheet_mock.call_count == 1

    from urllib.parse import parse_qs, urlparse
    parsed_url = urlparse(balance_sheet_mock.request_history[0].url)
    query_params = parse_qs(parsed_url.query)

    assert query_params.get("summarize_column_by", [None])[0] == "Month", \
        f"summarize_column_by should be 'Month' for Mode 1 reports, got {query_params}"

    assert query_params["accounting_method"][0] == "Accrual"
    assert "start_date" in query_params
    assert "end_date" in query_params


@freezegun.freeze_time(_NOW.isoformat())
def test_request_params_with_first_dimension(requests_mock, mock_firebase_client):
    """Test that summarize_column_by parameter IS included when first_dimension is set"""

    # Mock the OAuth token refresh endpoint
    requests_mock.post(
        "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
        json={"access_token": "fake-token", "expires_in": 3600, "token_type": "Bearer"}
    )

    # Mock the BalanceSheet API call
    balance_sheet_mock = requests_mock.get(
        "https://quickbooks.api.intuit.com/v3/company/123456789/reports/BalanceSheet",
        json=load_test_data("api_responses/balance_sheet_nguyen_with_classes_20240423.json")
    )

    # Config WITH first_dimension set to Classes
    config_with_first_dimension = {
        "realm_id": "123456789",
        "start_date": "2024-01-01",
        "end_date": "2024-01-31",
        "credentials": {
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
            "refresh_token": "test_refresh_token"
        },
        "accounting_method": {
            "selected_method": "Accrual"
        },
        "balance_sheet_settings": {
            "summarize_column": {
                "selected_first_dimension": "Classes"
            }
        }
    }

    source = SourceQuickbooksDrivepoint()
    streams = source.streams(config_with_first_dimension)

    # Find the BalanceSheet stream
    balance_sheet_stream = None
    for stream in streams:
        if hasattr(stream, '__class__') and "BalanceSheet" in stream.__class__.__name__:
            balance_sheet_stream = stream
            break

    assert balance_sheet_stream is not None, "BalanceSheet stream not found"
    assert balance_sheet_stream.first_dimension == "Classes", "first_dimension should be 'Classes'"

    # Read records to trigger the API call
    records = list(balance_sheet_stream.read_records(sync_mode="full_refresh"))

    # Verify the API was called
    assert balance_sheet_mock.call_count == 1, "BalanceSheet endpoint should be called once"

    # Get the actual request that was made
    actual_request = balance_sheet_mock.request_history[0]

    # Parse query string - requests_mock stores it as a string
    from urllib.parse import parse_qs, urlparse
    parsed_url = urlparse(actual_request.url)
    query_params = parse_qs(parsed_url.query)

    # Verify that summarize_column_by IS in the query parameters
    assert "summarize_column_by" in query_params, \
        f"summarize_column_by should be in query params when first_dimension is set. Query params: {query_params}"

    # Verify it has the correct value (parse_qs returns lists, so get first item)
    assert query_params["summarize_column_by"][0] == "Classes", \
        f"summarize_column_by should be 'Classes', got '{query_params.get('summarize_column_by', [''])[0]}'"


@freezegun.freeze_time(_NOW.isoformat())
def test_fallback_mode_no_duplicate_records_across_months(requests_mock, mock_firebase_client):
    """
    Test for duplicate records in fallback mode across multiple monthly slices.

    Setup:
      - 2 monthly slices (Jan + Feb 2024)
      - first_dimension = Classes (2 classes: Retail, Wholesale)
      - balance_sheet_fallback_batched.json has 1 account row × 2 class columns
        → 2 records per slice
      - each fallback slice also pulls the plain total report
        (balance_sheet_total_plain.json, 1 account row) → 1 DRIVEPOINT_CLASS_TOTAL
        record per slice

    Expected: 2 slices × (2 class + 1 total) records = 6 total
    """

    requests_mock.post(
        "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
        json={"access_token": "fake-token", "expires_in": 3600, "token_type": "Bearer"}
    )

    result_set_big_error = {
        "Fault": {
            "Error": [{
                "Message": "Result Set Big Error",
                "Detail": "Report too large, please customize",
                "code": RESULT_SET_BIG_ERROR_CODE,
                "element": "ReportName"
            }],
            "type": "ValidationFault"
        }
    }

    def report_callback(request, context):
        from urllib.parse import parse_qs, urlparse
        query_params = parse_qs(urlparse(request.url).query)

        # Normal-mode probe for any slice: summarize_column_by set but no class filter —
        # error every time so each monthly slice falls back (keeps the test symmetric).
        if "summarize_column_by" in query_params and "class" not in query_params:
            context.status_code = 400
            return result_set_big_error

        # Plain total call: no summarize_column_by at all — the DRIVEPOINT_CLASS_TOTAL recovery
        if "summarize_column_by" not in query_params:
            return load_test_data("api_responses/balance_sheet_total_plain.json")

        # Batched fallback call with class filter — return success
        return load_test_data("api_responses/balance_sheet_fallback_batched.json")

    requests_mock.get(
        "https://quickbooks.api.intuit.com/v3/company/123456789/reports/BalanceSheet",
        json=report_callback
    )

    requests_mock.get(
        "https://quickbooks.api.intuit.com/v3/company/123456789/query",
        json=load_test_data("api_responses/classes_query_fallback.json")
    )

    config = {
        "realm_id": "123456789",
        "start_date": "2024-01-01",
        "end_date": "2024-02-29",  # Two months — critical for exposing the bug
        "credentials": {
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
            "refresh_token": "test_refresh_token"
        },
        "accounting_method": {"selected_method": "Accrual"},
        "balance_sheet_settings": {
            "summarize_column": {"selected_first_dimension": "Classes"}
        }
    }

    source = SourceQuickbooksDrivepoint()
    streams = source.streams(config)
    balance_sheet_stream = next(
        s for s in streams if "BalanceSheet" in s.__class__.__name__
    )

    # Simulate what the Airbyte framework does: call read_records once per slice
    all_slices = list(balance_sheet_stream.stream_slices(sync_mode="full_refresh"))
    assert len(all_slices) == 2, f"Expected 2 monthly slices, got {len(all_slices)}"

    all_records = []
    for stream_slice in all_slices:
        slice_records = list(balance_sheet_stream.read_records(
            sync_mode="full_refresh",
            stream_slice=stream_slice
        ))
        all_records.extend(slice_records)

    # 2 months × (1 account × 2 classes + 1 plain total) = 6 records
    # With the duplicate bug: each slice would re-emit every other slice's records.
    records_per_slice = 3  # 2 class columns (batched) + 1 DRIVEPOINT_CLASS_TOTAL (plain total)
    expected_total = len(all_slices) * records_per_slice
    assert len(all_records) == expected_total, (
        f"Expected {expected_total} records ({len(all_slices)} slices × {records_per_slice} records/slice), "
        f"got {len(all_records)}. "
        f"A larger count would indicate the duplicate-records bug."
    )

    # Verify fallback was triggered for at least one period (items cache populated by fallback logic)
    assert balance_sheet_stream._fallback_mode_first_dimension_items is not None, \
        "Fallback should have been triggered and dimension items cached"
    # Fallback flag is reset to False after each period — fallback is per-period, not sticky
    assert balance_sheet_stream._first_dimension_fallback_mode is False

    # Verify both classes plus the plain-total magic row appear in records
    class_values = {r.get("Class") for r in all_records}
    assert "Retail" in class_values
    assert "Wholesale" in class_values
    assert "DRIVEPOINT_CLASS_TOTAL" in class_values, \
        "Fallback should emit a DRIVEPOINT_CLASS_TOTAL row so 'Not Specified' can be derived downstream"


@freezegun.freeze_time(_NOW.isoformat())
def test_result_set_big_error_fallback(requests_mock, mock_firebase_client):
    """Test that when QuickBooks returns ResultSetBigError (10100), the connector
    falls back to fetching dimension items in batches with adaptive batch sizing"""

    # Mock the OAuth token refresh endpoint
    requests_mock.post(
        "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
        json={"access_token": "fake-token", "expires_in": 3600, "token_type": "Bearer"}
    )

    # Error response for the initial request with summarize_column_by (no filter)
    result_set_big_error = {
        "Fault": {
            "Error": [{
                "Message": "Result Set Big Error",
                "Detail": "Result Set Big Error : Report too large, please customize",
                "code": RESULT_SET_BIG_ERROR_CODE,
                "element": "ReportName"
            }],
            "type": "ValidationFault"
        }
    }

    # Track how many times the report API was called and with what params
    report_call_count = [0]
    first_call_done = [False]

    def report_callback(request, context):
        report_call_count[0] += 1
        from urllib.parse import parse_qs, urlparse
        parsed_url = urlparse(request.url)
        query_params = parse_qs(parsed_url.query)

        # First call has summarize_column_by but no class filter - return error to trigger fallback
        if "summarize_column_by" in query_params and "class" not in query_params and not first_call_done[0]:
            first_call_done[0] = True
            context.status_code = 400  # QuickBooks returns 400 for this error
            return result_set_big_error

        # Fallback calls have summarize_column_by AND class filter (comma-separated IDs)
        if "summarize_column_by" in query_params and "class" in query_params:
            # Return batched response with columns for each class in the batch
            return load_test_data("api_responses/balance_sheet_fallback_batched.json")

        # Plain total call: no summarize_column_by — the DRIVEPOINT_CLASS_TOTAL recovery
        if "summarize_column_by" not in query_params:
            return load_test_data("api_responses/balance_sheet_total_plain.json")

        # Shouldn't reach here in normal flow
        return load_test_data("api_responses/balance_sheet_simple.json")

    balance_sheet_mock = requests_mock.get(
        "https://quickbooks.api.intuit.com/v3/company/123456789/reports/BalanceSheet",
        json=report_callback
    )

    # Mock the Classes query endpoint (for fetching dimension items)
    classes_query_mock = requests_mock.get(
        "https://quickbooks.api.intuit.com/v3/company/123456789/query",
        json=load_test_data("api_responses/classes_query_fallback.json")
    )

    config_with_classes = {
        "realm_id": "123456789",
        "start_date": "2024-01-01",
        "end_date": "2024-01-31",  # Single month
        "credentials": {
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
            "refresh_token": "test_refresh_token"
        },
        "accounting_method": {
            "selected_method": "Accrual"
        },
        "balance_sheet_settings": {
            "summarize_column": {
                "selected_first_dimension": "Classes"
            }
        }
    }

    source = SourceQuickbooksDrivepoint()
    streams = source.streams(config_with_classes)

    # Find the BalanceSheet stream
    balance_sheet_stream = None
    for stream in streams:
        if hasattr(stream, '__class__') and "BalanceSheet" in stream.__class__.__name__:
            balance_sheet_stream = stream
            break

    assert balance_sheet_stream is not None, "BalanceSheet stream not found"
    assert balance_sheet_stream.first_dimension == "Classes"
    assert balance_sheet_stream._first_dimension_fallback_mode == False, "Should not be in fallback mode initially"

    # Read records - should trigger fallback (pass stream_slice as the framework does)
    slices = list(balance_sheet_stream.stream_slices(sync_mode="full_refresh"))
    assert len(slices) == 1, f"Expected 1 monthly slice, got {len(slices)}"
    records = list(balance_sheet_stream.read_records(sync_mode="full_refresh", stream_slice=slices[0]))

    # Verify fallback was triggered (items cache populated); flag itself is reset to False after each period
    assert balance_sheet_stream._fallback_mode_first_dimension_items is not None, \
        "Fallback should have been triggered and dimension items cached"
    assert balance_sheet_stream._first_dimension_fallback_mode is False, \
        "Fallback flag should be reset to False after period completes (per-period fallback, not sticky)"

    # Verify the API was called:
    # 1 initial call (returns error) + 1 batched call (both classes) + 1 plain total call
    assert report_call_count[0] >= 3, f"Expected at least 3 report API calls (1 error + 1 batch + 1 total), got {report_call_count[0]}"

    # Verify classes query was called to get dimension items
    assert classes_query_mock.call_count == 1, "Classes query should be called once to get dimension items"

    # Verify we got records
    assert len(records) > 0, "Should have received records after fallback"

    # Verify Class field is set correctly on records
    class_values = set(r.get("Class") for r in records)
    # Should have records for each class (Retail and Wholesale)
    assert "Retail" in class_values, "Should have Retail records"
    assert "Wholesale" in class_values, "Should have Wholesale records"
    # And the plain-total magic row from the fallback recovery of untagged ("Not Specified") dollars
    assert "DRIVEPOINT_CLASS_TOTAL" in class_values, "Should have a DRIVEPOINT_CLASS_TOTAL row"


# ---------------------------------------------------------------------------
# Incremental sync
# ---------------------------------------------------------------------------

from airbyte_cdk.models import SyncMode
from airbyte_cdk.test.catalog_builder import CatalogBuilder
from airbyte_cdk.test.entrypoint_wrapper import discover, read
from airbyte_cdk.test.state_builder import StateBuilder

_PANL_STREAM = "profit_loss_report_monthly"
_PANL_URL = "https://quickbooks.api.intuit.com/v3/company/123456789/reports/ProfitAndLoss"


def _second_dimension_config(**overrides):
    # Must be valid against spec.json since it goes through the CDK entrypoint
    config = {
        "realm_id": "123456789",
        "company_id": "test_company",
        "client_id": "test_client_id",
        "client_secret": "test_client_secret",
        "start_date": "2024-01-01T00:00:00Z",
        "end_date": "2024-03-31T00:00:00Z",
        "accounting_method": {"selected_method": "Accrual"},
        "profit_loss_settings": {
            "summarize_column": {"selected_first_dimension": "Classes"},
            "second_dimension": {"selected_second_dimension": "Departments"}
        }
    }
    config.update(overrides)
    return {k: v for k, v in config.items() if v is not None}


def _mock_second_dimension_api(requests_mock):
    """Mock token, Departments query and P&L; returns the list of (start_date, department) P&L calls."""
    requests_mock.post(
        "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
        json={"access_token": "fake-token", "expires_in": 3600, "token_type": "Bearer"}
    )
    requests_mock.get(
        "https://quickbooks.api.intuit.com/v3/company/123456789/query",
        json=load_test_data("api_responses/departments_query.json")
    )

    calls = []

    def report_callback(request, context):
        calls.append((request.qs["start_date"][0], request.qs.get("department", [None])[0]))
        context.status_code = 200
        return load_test_data("api_responses/pandl_with_classes_second_dimension_TOTAL.json")

    requests_mock.get(_PANL_URL, json=report_callback)
    return calls


def _panl_catalog(sync_mode):
    return CatalogBuilder().with_stream(_PANL_STREAM, sync_mode).build()


def _state_dict(stream_state):
    return {k: v for k, v in stream_state.__dict__.items()}


def test_report_streams_primary_key_and_incremental_support(requests_mock, mock_firebase_client):
    output = discover(SourceQuickbooksDrivepoint(), _second_dimension_config())
    streams = {s.name: s for s in output.catalog.catalog.streams}

    for name in ("profit_loss_report_monthly", "balance_sheet_report_monthly"):
        assert streams[name].source_defined_primary_key == [["_Account_id"], ["Class"], ["Dimension1"], ["StartPeriod"]]
        assert SyncMode.incremental in streams[name].supported_sync_modes
        assert streams[name].default_cursor_field == ["StartPeriod"]

    assert streams["transaction_list_report_monthly"].supported_sync_modes == [SyncMode.full_refresh]


@freezegun.freeze_time(_NOW.isoformat())
def test_incremental_first_sync_second_dimension_calls_each_report_once(requests_mock, mock_firebase_client):
    """Through the real CDK read path (one read_records call per slice), every
    (month, department) report is requested exactly once and state ends on the last month."""
    calls = _mock_second_dimension_api(requests_mock)

    output = read(SourceQuickbooksDrivepoint(), _second_dimension_config(), _panl_catalog(SyncMode.incremental))

    expected_calls = [(month, dept) for month in ("2024-01-01", "2024-02-01", "2024-03-01") for dept in (None, "1", "2")]
    assert calls == expected_calls
    # 3 months x 3 reports (TOTAL + 2 departments) x 5 accounts x 3 classes
    assert len(output.records) == 135
    assert _state_dict(output.most_recent_state.stream_state) == {"StartPeriod": "2024-03-01T00:00:00Z"}
    # State is checkpointed after each month
    checkpoints = [_state_dict(m.state.stream.stream_state).get("StartPeriod") for m in output.state_messages]
    assert checkpoints[:3] == ["2024-01-01T00:00:00Z", "2024-02-01T00:00:00Z", "2024-03-01T00:00:00Z"]


@freezegun.freeze_time(_NOW.isoformat())
def test_incremental_sync_with_state_only_rereads_lookback_window(requests_mock, mock_firebase_client):
    calls = _mock_second_dimension_api(requests_mock)
    config = _second_dimension_config(end_date=None, incremental_lookback_months=1)
    state = StateBuilder().with_stream_state(_PANL_STREAM, {"StartPeriod": "2024-05-01T00:00:00Z"}).build()

    output = read(SourceQuickbooksDrivepoint(), config, _panl_catalog(SyncMode.incremental), state)

    # Cursor month (May) minus 1 month of lookback, up to today (2024-06-15)
    assert sorted({month for month, _ in calls}) == ["2024-04-01", "2024-05-01", "2024-06-01"]
    assert len(calls) == 9
    assert _state_dict(output.most_recent_state.stream_state) == {"StartPeriod": "2024-06-01T00:00:00Z"}


@freezegun.freeze_time(_NOW.isoformat())
def test_full_refresh_ignores_state(requests_mock, mock_firebase_client):
    calls = _mock_second_dimension_api(requests_mock)
    state = StateBuilder().with_stream_state(_PANL_STREAM, {"StartPeriod": "2024-03-01T00:00:00Z"}).build()

    read(SourceQuickbooksDrivepoint(), _second_dimension_config(), _panl_catalog(SyncMode.full_refresh), state)

    assert sorted({month for month, _ in calls}) == ["2024-01-01", "2024-02-01", "2024-03-01"]
    assert len(calls) == 9


@freezegun.freeze_time(_NOW.isoformat())
def test_incremental_slices_respect_start_date_and_monthly_columns(mock_firebase_client):
    config = {**_CONFIG, "start_date": "2024-03-01", "end_date": None, "incremental_lookback_months": 6}
    pandl = {type(s).__name__: s for s in SourceQuickbooksDrivepoint().streams(config)}["ProfitLossReportMonthly"]

    # Mode 1 (no dimensions) uses yearly slices; the lookback never goes before start_date
    slices = pandl.stream_slices(sync_mode=SyncMode.incremental, stream_state={"StartPeriod": "2024-05-01T00:00:00Z"})
    assert slices == [{"start_date": "2024-03-01", "end_date": "2024-06-15"}]

    # A yearly slice advances the cursor to its last month, not to January
    pandl.state = {}
    pandl._advance_cursor({"start_date": "2024-01-01", "end_date": "2024-06-15"})
    assert pandl.state == {"StartPeriod": "2024-06-01T00:00:00Z"}

    # Lookback crossing a year boundary
    config = {**_CONFIG, "start_date": "2020-01-01", "end_date": None, "incremental_lookback_months": 6}
    pandl = {type(s).__name__: s for s in SourceQuickbooksDrivepoint().streams(config)}["ProfitLossReportMonthly"]
    slices = pandl.stream_slices(sync_mode=SyncMode.incremental, stream_state={"StartPeriod": "2024-03-01T00:00:00Z"})
    assert slices == [
        {"start_date": "2023-09-01", "end_date": "2023-12-31"},
        {"start_date": "2024-01-01", "end_date": "2024-06-15"},
    ]


@freezegun.freeze_time(_NOW.isoformat())
def test_transaction_list_reads_through_cdk_entrypoint(requests_mock, mock_firebase_client):
    """The CDK assigns stream.state before reading every stream; TransactionList has no
    cursor and must accept that (regression: TypeError unhashable type 'list')."""
    _mock_second_dimension_api(requests_mock)
    requests_mock.get(
        "https://quickbooks.api.intuit.com/v3/company/123456789/reports/TransactionList",
        json=load_test_data("api_responses/transaction_list_simple.json")
    )
    catalog = CatalogBuilder().with_stream("transaction_list_report_monthly", SyncMode.full_refresh).build()

    output = read(SourceQuickbooksDrivepoint(), _second_dimension_config(), catalog)

    assert not output.errors, [e.trace.error.message for e in output.errors]
    assert len(output.records) == len(load_test_data("expected_results/transaction_list_simple.json"))
