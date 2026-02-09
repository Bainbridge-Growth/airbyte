import json
import os
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

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
    """Test that summarize_column_by parameter is NOT included when first_dimension is None"""

    # Mock the OAuth token refresh endpoint
    requests_mock.post(
        "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
        json={"access_token": "fake-token", "expires_in": 3600, "token_type": "Bearer"}
    )

    # Mock the BalanceSheet API call - we'll inspect what was actually requested
    balance_sheet_mock = requests_mock.get(
        "https://quickbooks.api.intuit.com/v3/company/123456789/reports/BalanceSheet",
        json=load_test_data("api_responses/balance_sheet_simple.json")
    )

    # Config WITHOUT first_dimension (no summarize_column settings)
    config_without_first_dimension = {
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
        }
        # Note: no balance_sheet_settings with summarize_column
    }

    source = SourceQuickbooksDrivepoint()
    streams = source.streams(config_without_first_dimension)

    # Find the BalanceSheet stream
    balance_sheet_stream = None
    for stream in streams:
        if hasattr(stream, '__class__') and "BalanceSheet" in stream.__class__.__name__:
            balance_sheet_stream = stream
            break

    assert balance_sheet_stream is not None, "BalanceSheet stream not found"
    assert balance_sheet_stream.first_dimension is None, "first_dimension should be None"

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

    # Verify that summarize_column_by is NOT in the query parameters
    assert "summarize_column_by" not in query_params, \
        f"summarize_column_by should NOT be in query params when first_dimension is None. Query params: {query_params}"

    # Verify that other expected params ARE present
    assert "accounting_method" in query_params, "accounting_method should be present"
    assert query_params["accounting_method"][0] == "Accrual", "accounting_method should be Accrual"
    assert "start_date" in query_params, "start_date should be present"
    assert "end_date" in query_params, "end_date should be present"


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


