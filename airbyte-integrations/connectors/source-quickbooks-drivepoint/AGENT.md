# Agent Instructions for source-quickbooks-drivepoint Connector

## Overview
This is a custom Airbyte connector that extracts financial reports from QuickBooks API for Drivepoint. It implements monthly chunking, multi-dimensional reporting (first and second dimensions), and integrates with Firebase for authentication and Google Cloud Secret Manager for credentials.

## Architecture

### Core Components

1. **report_streams.py** - Report API streams (Balance Sheet, P&L)
   - `QuickbooksReportMonthlyBase` - Base class for report streams
   - `BalanceSheetReportMonthly` - Balance Sheet implementation
   - `ProfitLossReportMonthly` - Profit & Loss implementation

2. **query_streams.py** - Query API streams for dimension data
   - `QueryStreamBase` - Base class for query streams
   - `Classes`, `Departments`, `Customers`, `Vendors`, `Accounts` - Entity streams

3. **auth_client.py** - OAuth2 authentication with QuickBooks API
   - Integrates with Firebase for token management
   - Uses Google Cloud Secret Manager for credentials

4. **source.py** - Main source implementation
   - Stream initialization
   - Configuration parsing
   - Connection testing

## Key Concepts

### Monthly Chunking
Reports are fetched in monthly chunks from `start_date` to `end_date` (or today if not specified). This is handled by `stream_slices()` in `QuickbooksReportMonthlyBase`.

### Multi-Dimensional Reporting

**First Dimension** (`first_dimension`):
- Configured via `summarize_column_by` parameter in QuickBooks API
- Options: `None`, `Classes`, `Departments`, `Customers`, `Vendors`
- Controls column grouping in the report

**Second Dimension** (`second_dimension`):
- Requires `first_dimension` to be set (cannot be used alone)
- Fetches dimension items from query endpoint once per sync
- For each dimension item, requests a separate report filtered by that dimension ID
- Also fetches one "TOTAL" report without dimension filter (labeled as `DRIVEPOINT_CLASS_TOTAL`)
- Adds `Dimension1` field with dimension name to each record
- Processing logic in `read_records()` method

### Report Processing Flow

1. **Without second_dimension:**
   - Call report API with monthly slices
   - Parse hierarchical JSON response into flat records
   - Each record includes account hierarchy (category, grandparent, parent, account)

2. **With second_dimension:**
   - Fetch all dimension items from query endpoint ONCE per sync
   - Extract distinct Id->Name pairs
   - For each monthly slice:
     - First, fetch TOTAL report (no dimension filter) with `Dimension1 = "DRIVEPOINT_CLASS_TOTAL"`
     - Then, for each dimension item:
       - Fetch report with dimension filter (e.g., `department=123`)
       - Add `Dimension1` field with dimension name
       - Track processed IDs to detect and prevent duplicates

### Response Parsing

Reports have hierarchical structure:
- **Section** rows - grouping headers (Assets, Liabilities, Income, etc.)
- **Data** rows - actual account data with values
- **Header** accounts - parent/grouping accounts (e.g., "4000 Sales of Product Income")
- **Detail** accounts - leaf accounts with actual transactions

Parser (`_process_rows()`) recursively processes this hierarchy and creates flat records with:
- Account hierarchy fields: `CategoryName`, `SectionType`, `GrandParentAccountName`, `ParentAccountName`, `_Account`
- Multiple value columns based on `first_dimension` (one per class/department/etc.)
- Metadata: `StartPeriod`, `EndPeriod`, `Currency`, `_airbyte_emitted_at`
- Second dimension: `Dimension1` (if applicable)

**Important:** "TOTAL" columns in reports with first_dimension are skipped to avoid duplication.

## Testing

### Test Structure
- **unit_tests/test_source.py** - Main test file
- **unit_tests/resources/api_responses/** - Mock API responses
- **unit_tests/resources/expected_results/** - Expected output records

### Test Naming Convention
- `test_<report>_<description>` - e.g., `test_balance_sheet_simple`, `test_pandl_with_classes_second_dimension`

### Creating Tests

1. Create API response file in `unit_tests/resources/api_responses/`
2. Create expected results file in `unit_tests/resources/expected_results/`
3. Use `source_full_refresh_and_compare()` helper function
4. Mock requests with `requests_mock` fixture

Example test structure:
```python
def test_report_name(requests_mock, mock_firebase_client):
    # Mock OAuth token
    requests_mock.post("https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer", ...)
    
    # Mock report API - can be called multiple times with matcher
    def custom_matcher(request):
        # Check for specific parameters
        return "param" in request.url
    
    requests_mock.get(
        "https://quickbooks.api.intuit.com/v3/company/123456789/reports/BalanceSheet",
        [
            {'json': load_test_data("api_responses/file1.json"), 'status_code': 200},
            {'json': load_test_data("api_responses/file2.json"), 'status_code': 200},
        ],
        additional_matcher=custom_matcher
    )
    
    # Run test
    source_full_refresh_and_compare("BalanceSheet", requests_mock, mock_firebase_client, 
                                   "test_data.json", expected_count, rows_to_check)
```

### Key Test Scenarios
- Simple reports without dimensions
- Reports with first_dimension only
- Reports with both first and second dimensions
- Edge cases: deeply nested accounts, Net Income handling, TOTAL column skipping
- Request parameter validation (ensure `summarize_column_by` not included when None)

## Common Issues & Solutions

### Issue: Duplicate API Calls
**Symptom:** Same API request made twice for the same parameters
**Cause:** Logic error in `read_records()` or dimension processing
**Solution:** Check `processed_ids` set and ensure `current_dimension_id` is properly tracked

### Issue: Test Hangs
**Symptom:** Test doesn't complete, appears to hang
**Cause:** Missing mock for API request
**Solution:** Add `requests_mock.get()` for all expected API calls, check logs for unmocked URLs

### Issue: Missing Data in Records
**Symptom:** Expected fields are empty or missing
**Cause:** Parser not handling hierarchy correctly
**Solution:** Check `_process_rows()` logic for section types and account hierarchy

### Issue: Wrong Column Values
**Symptom:** Values from wrong columns appear in records
**Cause:** Column mapping or TOTAL column handling issue
**Solution:** Verify `column_classes` list and TOTAL skipping logic in `parse_response()`

### Issue: Authentication Errors
**Symptom:** 401/403 errors in production
**Cause:** Token refresh issues or Firebase configuration
**Solution:** Check Firebase client, Secret Manager access, and OAuth token expiration

## Configuration

### Config Structure
```json
{
  "company_id": "drivepoint_company_id",
  "client_id": "oauth_client_id",
  "client_secret": "oauth_client_secret",
  "accounting_method": {
    "selected_method": "Accrual"  // or "Cash"
  },
  "balance_sheet_settings": {
    "summarize_column": {
      "selected_first_dimension": "Classes"  // or None, Departments, Customers, Vendors
    },
    "second_dimension": {
      "selected_second_dimension": "Departments"  // Requires first_dimension to be set
    }
  },
  "profit_loss_settings": {
    // Similar structure to balance_sheet_settings
  },
  "start_date": "2015-01-01",
  "end_date": "2024-12-31"  // Optional, defaults to today
}
```

### Important Config Rules
- `second_dimension` requires `first_dimension` to be set
- `start_date` determines how far back to pull data (monthly chunks)
- `accounting_method` affects report calculations (Accrual vs Cash basis)

## Development Workflow

### Local Development
```bash
# Install dependencies
poetry install --with dev

# Run tests
poetry run pytest unit_tests

# Run specific test
poetry run pytest unit_tests/test_source.py::test_name -v

# Run connector locally
poetry run source-quickbooks-drivepoint read --config secrets/config.json --catalog integration_tests/configured_catalog.json
```

### Debugging Tips

1. **Enable detailed logging:** Check logs for "Request URL:" and response details
2. **Use `logger.info()` liberally:** Track dimension processing, API calls, record counts
3. **Test with small date ranges:** Use 1-2 months for faster iteration
4. **Mock all API calls:** Use `requests_mock` to control responses
5. **Compare test data structure:** Ensure test API responses match production structure
6. **Check for duplicate processing:** Look for "DUPLICATE PROCESSING DETECTED" logs

## Performance Considerations

1. **Second dimension items fetched once:** Query endpoint called once per sync, not per month
2. **Monthly chunking:** Reduces memory usage for large date ranges
3. **Flat record structure:** Avoids nested JSON for easier querying
4. **API rate limits:** QuickBooks has rate limits, connector logs all requests
5. **Pagination:** Query streams use STARTPOSITION/MAXRESULTS (1000 records per page)

## Code Style & Patterns

- Use `logger.info()` for key operations (API calls, dimension processing)
- Use `logger.debug()` for detailed traces (query parameters, column mappings)
- Use `logger.error()` for errors with context
- Clean IDs with `clean_id()` to remove " at index X" suffixes
- Format dates consistently with `format_date()` helper
- Use `_airbyte_emitted_at` for record timestamps
- Prefix internal fields with underscore (e.g., `_Account`, `_Account_id`)

## Critical Code Sections

### `read_records()` in QuickbooksReportMonthlyBase
- Handles second dimension logic
- Fetches dimension items once
- Processes TOTAL report first, then filtered reports
- Tracks processed IDs to prevent duplicates

### `_process_rows()` in QuickbooksReportMonthlyBase  
- Recursively processes hierarchical report structure
- Builds account hierarchy (category -> section -> grandparent -> parent -> account)
- Creates flat records with all hierarchy levels
- Handles both header and detail accounts

### `_create_account_records()` in QuickbooksReportMonthlyBase
- Maps column data to record fields
- Adds dimension information if second_dimension is set
- Handles special cases (Net Income, empty values)

### Concurrency
- QuickBooks API has rate limits
- Connector is designed for sequential processing (one connection at a time)
- Multiple connections may queue if sharing same QuickBooks account
- This is by design to avoid hitting API rate limits
