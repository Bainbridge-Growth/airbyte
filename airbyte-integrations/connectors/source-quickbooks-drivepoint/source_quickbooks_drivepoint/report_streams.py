import pendulum
import requests
import logging
from datetime import datetime
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.models import AirbyteStateMessage, SyncMode
from airbyte_cdk.sources.streams.http.error_handlers import ErrorHandler, ErrorResolution, ResponseAction, HttpStatusErrorHandler
from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.streams.http.http_client import MessageRepresentationAirbyteTracedErrors
from .query_streams import Classes, Departments, Customers, Vendors

logger = logging.getLogger("airbyte")

# QuickBooks API error codes
RESULT_SET_BIG_ERROR_CODE = "10100"


class ResultSetBigError(Exception):
    """Raised when QuickBooks returns error 10100 (Result Set Big Error)"""
    pass


class QuickBooksReportErrorHandler(ErrorHandler):
    """Custom error handler that allows 10100 errors to pass through for handling in parse_response"""

    def __init__(self, logger, max_retries: int = 5, max_time: int = 600):
        self._logger = logger
        self._max_retries = max_retries
        self._max_time = max_time

    @property
    def max_retries(self) -> int:
        return self._max_retries

    @property
    def max_time(self) -> int:
        return self._max_time

    def interpret_response(self, response: Optional[requests.Response] = None) -> ErrorResolution:
        if response is None:
            return ErrorResolution(
                response_action=ResponseAction.RETRY,
                failure_type=FailureType.transient_error,
                error_message="No response received"
            )

        # Check if this is a 400 error with 10100 code
        if response.status_code == 400:
            try:
                response_json = response.json()
                fault = response_json.get("Fault", {})
                errors = fault.get("Error", [])
                for error in errors:
                    if error.get("code") == RESULT_SET_BIG_ERROR_CODE:
                        # This is ResultSetBigError - let it pass through to parse_response
                        self._logger.info(f"Detected ResultSetBigError ({RESULT_SET_BIG_ERROR_CODE}), allowing response to pass through")
                        return ErrorResolution(
                            response_action=ResponseAction.SUCCESS,
                            failure_type=None,
                            error_message=None
                        )
            except Exception:
                pass

        # Default error handling for other status codes
        if response.status_code == 429:
            return ErrorResolution(
                response_action=ResponseAction.RATE_LIMITED,
                failure_type=FailureType.transient_error,
                error_message="Rate limited"
            )
        elif response.status_code >= 500:
            return ErrorResolution(
                response_action=ResponseAction.RETRY,
                failure_type=FailureType.transient_error,
                error_message=f"Server error: {response.status_code}"
            )
        elif response.status_code >= 400:
            return ErrorResolution(
                response_action=ResponseAction.FAIL,
                failure_type=FailureType.system_error,
                error_message=f"Client error: {response.status_code}"
            )
        else:
            return ErrorResolution(
                response_action=ResponseAction.SUCCESS,
                failure_type=None,
                error_message=None
            )


# Convert date string to date-time format
def format_date(date_str):
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.strftime("%Y-%m-%dT00:00:00Z")
    except Exception:
        logger.error(f"Failed to parse date: {date_str}")
        return date_str  # fallback to original if parsing fails


# Clean ID by removing " at index X" suffix
def clean_id(id_str):
    """Remove ' at index X' suffix from IDs if present"""
    if id_str and " at index " in id_str:
        return id_str.split(" at index ")[0]
    return id_str

def get_dimension_query_param_name(dimension: str) -> str:
    mapping = {
        "Classes": "class",
        "Departments": "department",
        "Customers": "customer",
        "Vendors": "vendor"
    }
    return mapping.get(dimension)

def get_dimension_name_field(dimension: str) -> str:
    """Get the field name that contains the display name for a dimension type.

    QuickBooks uses different field names for names across entity types:
    - Classes: "Name"
    - Departments: "Name"
    - Customers: "DisplayName"
    - Vendors: "DisplayName"
    """
    mapping = {
        "Classes": "Name",
        "Departments": "Name",
        "Customers": "DisplayName",
        "Vendors": "DisplayName"
    }
    return mapping.get(dimension, "Name")

def get_query_stream_class(dimension: str):
    mapping = {
        "Classes": Classes,
        "Departments": Departments,
        "Customers": Customers,
        "Vendors": Vendors
    }
    return mapping.get(dimension)

class QuickbooksReportMonthlyBase(HttpStream):
    """Base class for QuickBooks Reports API connectors

    Reference: https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities
    """

    primary_key = ["_Account_id", "Class", "StartPeriod"]
    url_base = "https://quickbooks.api.intuit.com/v3/"
    # Disable automatic HTTP error raising so we can handle 400 errors with ResultSetBigError
    raise_on_http_errors = False

    def __init__(
            self,
            realm_id: str,  # company id
            accounting_method: str = "Accrual",
            first_dimension: str = None,
            second_dimension: str = None,
            start_date: str = None,
            end_date: str = None,
            authenticator = None,
            **kwargs
    ):
        self.realm_id = realm_id
        self.accounting_method = accounting_method
        self.first_dimension = first_dimension
        self.second_dimension = second_dimension
        self.start_date = start_date
        self.end_date = end_date
        self.current_dimension_id = None
        self.current_dimension_name = None
        self.authenticator = authenticator
        self._first_dimension_fallback_mode = False  # Set to True when ResultSetBigError is encountered
        self._first_dimension_filter_ids = None  # List of dimension IDs being filtered in fallback mode (for batching)
        self._fallback_batch_size = 1000  # Starting batch size for fallback mode
        self._fallback_mode_first_dimension_items = None  # Cache of dimension items for fallback mode
        super().__init__(authenticator=authenticator, **kwargs)

    def get_error_handler(self) -> Optional["ErrorHandler"]:
        """Override to provide custom error handler that allows 10100 errors to pass through"""
        if ErrorHandler is not None:
            return QuickBooksReportErrorHandler(logger=self.logger)
        return None

    def _fetch_all_dimension_items(self, query_stream) -> List[Mapping[str, Any]]:
        """Fetch all dimension items with explicit pagination handling.

        The CDK's read_records may not handle pagination correctly when called
        outside the normal sync context. This method explicitly paginates by
        making direct HTTP requests.
        """
        all_items = []

        # Get the stream slice (typically one slice with date range)
        slices = list(query_stream.stream_slices(sync_mode=None, cursor_field=None, stream_state=None))
        if not slices:
            slices = [None]

        for stream_slice in slices:
            next_page_token = None
            page_count = 0

            while True:
                page_count += 1
                # Get request parameters for this page
                params = query_stream.request_params(
                    stream_state=None,
                    stream_slice=stream_slice,
                    next_page_token=next_page_token
                )

                # Build and send request
                url = f"{query_stream.url_base}{query_stream.path()}"
                headers = query_stream.request_headers()

                response = requests.get(url, params=params, headers={
                    **headers,
                    "Authorization": f"Bearer {self.authenticator.get_access_token()}"
                })

                if response.status_code != 200:
                    self.logger.error(f"Error fetching dimension items: {response.status_code} - {response.text}")
                    break

                # Parse response
                records = list(query_stream.parse_response(response))
                all_items.extend(records)

                # Check if there are more pages
                next_page_token = query_stream.next_page_token(response)
                if not next_page_token:
                    break

        self.logger.info(f"Fetched total of {len(all_items)} items across {page_count} pages")
        return all_items

    def _get_dimension_items(self, dimension: str) -> Optional[List[Mapping[str, Any]]]:
        """Fetch all items for a dimension type.

        Args:
            dimension: The dimension type (Classes, Departments, Customers, Vendors)

        Returns:
            List of dimension items, or None if the dimension type is unknown
        """
        query_stream_class = get_query_stream_class(dimension)
        if not query_stream_class:
            self.logger.error(f"Unknown dimension: {dimension}")
            return None

        # Don't pass start_date/end_date - we want ALL dimension items, not filtered by date
        # The query stream filters by Metadata.LastUpdatedTime, which would exclude
        # items not updated within the report's date range
        query_stream = query_stream_class(
            realm_id=self.realm_id,
            authenticator=self.authenticator
        )

        items = self._fetch_all_dimension_items(query_stream)
        self.logger.info(f"Fetched {len(items)} items for {dimension}")
        return items

    def _extract_distinct_dimension_pairs(self, items: List[Mapping[str, Any]], dimension: str, context: str = "") -> Mapping[str, str]:
        """Extract distinct Id->Name pairs from dimension items.

        Args:
            items: List of dimension items from query stream
            dimension: The dimension type (Classes, Departments, Customers, Vendors)
            context: Optional context string for logging (e.g., "fallback mode")

        Returns:
            Dict mapping normalized IDs to names
        """
        name_field = get_dimension_name_field(dimension)
        self.logger.info(f"Using name field '{name_field}' for {dimension}")

        distinct_items = {}
        for item in items:
            item_id = item.get("Id")
            item_name = item.get(name_field)
            if item_id and item_name:
                normalized_id = str(item_id)
                if normalized_id in distinct_items:
                    self.logger.info(
                        f"Duplicate {dimension} id found: {normalized_id} "
                        f"(existing name: '{distinct_items[normalized_id]}', new name: '{item_name}')"
                    )
                distinct_items[normalized_id] = item_name

        context_suffix = f" ({context})" if context else ""
        self.logger.info(f"Found {len(distinct_items)} distinct Id->Name pairs for {dimension}{context_suffix}")
        return distinct_items

    def stream_slices(
            self,
            sync_mode: SyncMode = None,
            cursor_field: List[str] = None,
            stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        """
        Create monthly chunks from start_date to today (or end_date if specified)
        """
        # If no start_date is provided, return a single slice with no dates
        if not self.start_date:
            return [{}]

        # Convert to datetime objects first for consistent handling
        if isinstance(self.start_date, str):
            # Handle both simple YYYY-MM-DD and ISO 8601 formats
            try:
                start_dt = datetime.strptime(self.start_date, "%Y-%m-%d")
            except ValueError:
                start_dt = datetime.strptime(self.start_date.split('T')[0], "%Y-%m-%d")
        else:
            start_dt = self.start_date

        if self.end_date:
            if isinstance(self.end_date, str):
                try:
                    end_dt = datetime.strptime(self.end_date, "%Y-%m-%d")
                except ValueError:
                    end_dt = datetime.strptime(self.end_date.split('T')[0], "%Y-%m-%d")
            else:
                end_dt = self.end_date
        else:
            end_dt = datetime.now()

        # Create new pendulum dates directly
        start = pendulum.datetime(start_dt.year, start_dt.month, start_dt.day)
        end = pendulum.datetime(end_dt.year, end_dt.month, end_dt.day)

        slices = []
        current_start = start

        while current_start <= end:
            year = current_start.year
            month = current_start.month

            # Calculate the end of the current month
            if month == 12:
                next_month_year = year + 1
                next_month = 1
            else:
                next_month_year = year
                next_month = month + 1

            # First day of next month minus one day gives us last day of current month
            next_month_first = pendulum.datetime(next_month_year, next_month, 1)
            current_end = next_month_first.add(days=-1)

            # If current_end is beyond our end date, use the end date
            if current_end > end:
                current_end = end

            slices.append({
                "start_date": current_start.format("YYYY-MM-DD"),
                "end_date": current_end.format("YYYY-MM-DD")
            })

            # Move to the first day of next month
            current_start = next_month_first

        return slices

    def request_params(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        params = {
            "accounting_method": self.accounting_method
        }

        # In fallback mode, use summarize_column_by with batched dimension filter
        if self._first_dimension_fallback_mode:
            if self._first_dimension_filter_ids:
                # Use summarize_column_by to get columns for each dimension in the batch
                params["summarize_column_by"] = self.first_dimension
                # Filter to only include dimensions in this batch (comma-separated IDs)
                param_name = get_dimension_query_param_name(self.first_dimension)
                if param_name:
                    params[param_name] = ",".join(self._first_dimension_filter_ids)
            # If no filter IDs, we're not making a request (shouldn't happen)
        elif self.first_dimension is not None and self.first_dimension != "None":
            # Normal mode: add summarize_column_by if first_dimension is set
            params["summarize_column_by"] = self.first_dimension

        # When second_dimension is set, use first_dimension as summarize_column_by
        # and filter by the current second_dimension item
        if self.second_dimension and self.second_dimension != "None" and self.current_dimension_id:
            # Filter by second_dimension item
            param_name = get_dimension_query_param_name(self.second_dimension)
            if param_name:
                params[param_name] = self.current_dimension_id

        # Use dates from the stream slice if available
        if stream_slice:
            if stream_slice.get("start_date"):
                # QuickBooks API expects dates in YYYY-MM-DD format
                params["start_date"] = stream_slice["start_date"]
            if stream_slice.get("end_date"):
                params["end_date"] = stream_slice["end_date"]
        # Fall back to class variables if slice doesn't have dates
        else:
            if self.start_date:
                params["start_date"] = pendulum.parse(self.start_date).date().strftime("%Y-%m-%d")
            if self.end_date:
                params["end_date"] = pendulum.parse(self.end_date).date().strftime("%Y-%m-%d")

        return params

    def _send_request(self, request, request_kwargs):
        response = self._session.send(request, **request_kwargs)
        return response

    def _reduce_batch_size(self, current_batch_size: int, source: str = "") -> tuple:
        """
        Reduce batch size by roughly 20% after receiving ResultSetBigError.

        Returns:
            tuple: (new_batch_size, should_skip_batch)
        """
        old_batch_size = current_batch_size
        new_batch_size = max(1, int(current_batch_size * 0.8))
        self._fallback_batch_size = new_batch_size

        source_suffix = f" ({source})" if source else ""
        self.logger.warning(f"ResultSetBigError{source_suffix} with batch size {old_batch_size}, reducing to {new_batch_size} and retrying")

        if new_batch_size == old_batch_size:
            self.logger.error(f"Cannot reduce batch size further (already at {new_batch_size}), giving up on this batch")
            return new_batch_size, True  # should_skip = True

        return new_batch_size, False  # should_skip = False

    def _read_records_with_first_dimension_fallback(self, sync_mode, cursor_field, stream_slice, stream_state):
        """
        Fallback mode: fetch first_dimension items in batches instead of all at once.
        This is triggered when QuickBooks returns ResultSetBigError (10100) for reports with too many dimension columns.

        Uses adaptive batch sizing: starts with a large batch and reduces by ~20% on each ResultSetBigError
        until a working batch size is found.
        """
        self.logger.info(f"Using fallback mode for first_dimension={self.first_dimension}")

        if not self._fallback_mode_first_dimension_items:
            # Fetch the dimension items
            first_dimension_items = self._get_dimension_items(self.first_dimension)
            if first_dimension_items is None:
                return

            # Extract distinct Id->Name pairs and convert to list for batching
            distinct_items_dict = self._extract_distinct_dimension_pairs(
                first_dimension_items, self.first_dimension, "fallback mode"
            )

            if not distinct_items_dict:
                self.logger.warning(f"No dimension items found for {self.first_dimension}, skipping fallback")
                return

            # Convert to list of tuples for batching
            self._fallback_mode_first_dimension_items = list(distinct_items_dict.items())

        distinct_items = self._fallback_mode_first_dimension_items

        # Get the parameter name for this dimension type
        param_name = get_dimension_query_param_name(self.first_dimension)

        # Determine initial batch size (min of configured batch size and total items)
        batch_size = min(self._fallback_batch_size, len(distinct_items))
        self.logger.info(f"Starting with batch size {batch_size} for {len(distinct_items)} dimension items")

        # Process only the stream_slice passed in — the framework calls read_records()
        # once per slice, so we must not re-iterate all slices here.
        i = 0
        while i < len(distinct_items):
            # Get the current batch
            batch_end = min(i + batch_size, len(distinct_items))
            batch = distinct_items[i:batch_end]
            batch_ids = [item_id for item_id, _ in batch]

            self.logger.info(f"Fetching report for batch of {len(batch)} {param_name}s (IDs: {batch_ids[0]}...{batch_ids[-1]}) for period {stream_slice.get('start_date')} to {stream_slice.get('end_date')}")

            # Set the batch IDs for request_params
            self._first_dimension_filter_ids = batch_ids

            try:
                # Fetch records for this batch
                records = []
                for record in super().read_records(sync_mode, cursor_field, stream_slice, stream_state):
                    records.append(record)

                # Batch succeeded, yield records
                yield from records

                # Move to next batch
                i = batch_end
                self.logger.info(f"Successfully fetched {len(records)} records for batch")

            except ResultSetBigError as e:
                batch_size, should_skip = self._reduce_batch_size(batch_size)
                if should_skip:
                    i = batch_end  # Skip this batch
                # Otherwise don't increment i - retry with smaller batch

            except Exception as e:
                # Check if this is a CDK exception containing the 10100 error
                error_str = str(e)
                if f"'{RESULT_SET_BIG_ERROR_CODE}'" in error_str or f'"{RESULT_SET_BIG_ERROR_CODE}"' in error_str or f"code': '{RESULT_SET_BIG_ERROR_CODE}'" in error_str:
                    batch_size, should_skip = self._reduce_batch_size(batch_size, "from CDK")
                    if should_skip:
                        i = batch_end
                else:
                    raise

        # Reset fallback state
        self._first_dimension_filter_ids = None

    def read_records(self, sync_mode, cursor_field=None, stream_slice=None, stream_state=None):
        # When second_dimension is set AND we haven't fetched dimension items yet,
        # we fetch dimension items and manage slicing ourselves.
        # We detect the initial call by checking if current_dimension_id is None.
        # Once we set current_dimension_id and call super().read_records(), subsequent
        # nested calls will have current_dimension_id set and will use normal flow.
        if self.second_dimension and self.second_dimension != "None" and self.current_dimension_id is None:
            # When second_dimension is provided, we need to handle slicing ourselves
            # This is the initial call - we haven't started processing dimensions yet

            # Get all stream slices for the entire period
            all_slices = list(self.stream_slices(sync_mode, cursor_field, stream_state))

            # Fetch the dimension items once for the entire period
            second_dimension_items = self._get_dimension_items(self.second_dimension)
            if second_dimension_items is None:
                return

            # Extract distinct Id->Name pairs
            distinct_items = self._extract_distinct_dimension_pairs(
                second_dimension_items, self.second_dimension
            )

            # Get the parameter name for this dimension type (class/department/customer/vendor)
            param_name = get_dimension_query_param_name(self.second_dimension)

            # For each time slice, first fetch the total (without dimension filter)
            # then fetch data for each dimension item
            for time_slice in all_slices:
                # First, fetch report without second_dimension filter to get totals
                self.logger.info(f"Fetching DRIVEPOINT_CLASS_TOTAL report (no {param_name} filter) for period {time_slice['start_date']} to {time_slice['end_date']}")

                # Set dimension info to indicate this is the total
                self.current_dimension_id = None  # No filter applied
                self.current_dimension_name = "DRIVEPOINT_CLASS_TOTAL"

                # Fetch records without dimension filter
                yield from super().read_records(sync_mode, cursor_field, time_slice, stream_state)

                # Track which dimension IDs we've processed for this time slice to detect duplicates
                processed_ids = set()

                # Now fetch for each distinct Id->Name pair with dimension filter
                for item_id, item_name in distinct_items.items():
                    if item_id in processed_ids:
                        self.logger.error(f"DUPLICATE PROCESSING DETECTED: {param_name}={item_id} (Name: {item_name}) for period {time_slice['start_date']} to {time_slice['end_date']} - SKIPPING!")
                        continue

                    processed_ids.add(item_id)
                    self.logger.info(f"Fetching report for {param_name}={item_id} (Name: {item_name}) for period {time_slice['start_date']} to {time_slice['end_date']}")

                    # Set current dimension info for use in request_params and _create_account_records
                    self.current_dimension_id = item_id
                    self.current_dimension_name = item_name

                    # Fetch records for this time slice with this dimension filter
                    yield from super().read_records(sync_mode, cursor_field, time_slice, stream_state)
        elif self._first_dimension_fallback_mode:
            # Already in fallback mode, use the fallback implementation
            yield from self._read_records_with_first_dimension_fallback(sync_mode, cursor_field, stream_slice, stream_state)
        else:
            # Normal flow: no second_dimension OR we're in a nested call with current_dimension_id already set
            if not self.second_dimension or self.second_dimension == "None":
                # Only clear dimension info if second_dimension is not configured
                self.current_dimension_id = None
                self.current_dimension_name = None

            # Try normal read, catch ResultSetBigError and fall back if needed
            try:
                records = []
                for record in super().read_records(sync_mode, cursor_field, stream_slice, stream_state):
                    records.append(record)

                # If we got here without error, yield all records
                yield from records
            except ResultSetBigError as e:
                # Report is too large with summarize_column_by, switch to fallback mode
                self.logger.warning(f"Switching to fallback mode due to ResultSetBigError: {e}")
                self._first_dimension_fallback_mode = True

                # Re-process with fallback mode
                yield from self._read_records_with_first_dimension_fallback(sync_mode, cursor_field, stream_slice, stream_state)
            except Exception as e:
                # Check if this is a CDK exception containing the 10100 error
                error_str = str(e)
                if f"'{RESULT_SET_BIG_ERROR_CODE}'" in error_str or f'"{RESULT_SET_BIG_ERROR_CODE}"' in error_str or f"code': '{RESULT_SET_BIG_ERROR_CODE}'" in error_str:
                    self.logger.warning(f"Switching to fallback mode due to ResultSetBigError detected in CDK exception: {e}")
                    self._first_dimension_fallback_mode = True

                    # Re-process with fallback mode
                    yield from self._read_records_with_first_dimension_fallback(sync_mode, cursor_field, stream_slice, stream_state)
                else:
                    raise

    def request_headers(
            self,
            stream_state: Mapping[str, Any] = None,
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> Mapping[str, Any]:
        return {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        # Handle HTTP errors since we have raise_on_http_errors = False
        try:
            response_json = response.json()
        except Exception as e:
            # Response is not valid JSON
            if not response.ok:
                self.logger.error(f"HTTP error {response.status_code}: {response.text}")
                response.raise_for_status()  # Re-raise the HTTP error
            raise e

        # Check for QuickBooks API errors (can occur with 400 status code)
        fault = response_json.get("Fault")
        if fault:
            errors = fault.get("Error", [])
            for error in errors:
                error_code = error.get("code")
                error_message = error.get("Message", "")
                error_detail = error.get("Detail", "")

                if error_code == RESULT_SET_BIG_ERROR_CODE:
                    # Result Set Big Error - report is too large
                    self.logger.warning(f"QuickBooks returned ResultSetBigError ({RESULT_SET_BIG_ERROR_CODE}): {error_message} - {error_detail}")
                    raise ResultSetBigError(f"{error_message}: {error_detail}")

                # Log other errors
                self.logger.error(f"QuickBooks API error {error_code}: {error_message} - {error_detail}")

            # For other errors, raise HTTP error to let Airbyte handle it
            if not response.ok:
                response.raise_for_status()
            return []

        # If response is not OK and no Fault, raise the error
        if not response.ok:
            self.logger.error(f"Unexpected HTTP error {response.status_code}: {response.text}")
            response.raise_for_status()

        header = response_json.get("Header", {})
        rows = response_json.get("Rows", {}).get("Row", [])
        columns = response_json.get("Columns", {}).get("Column", [])

        if not rows:
            self.logger.warning("No rows found in balance sheet response")
            return []

        request_url = response.request.url
        self.logger.info(f"Request URL: {request_url}")

        start_period = format_date(header.get("StartPeriod"))
        end_period = format_date(header.get("EndPeriod"))
        currency = header.get("Currency")

        # Build column mapping (skip first column which is account name)
        column_classes = []
        for i, col in enumerate(columns[1:], 1):  # Skip first column (Account)
            col_title = col.get("ColTitle", "")
            class_name = col_title if col_title else f"Column_{i}"

            if len(column_classes) >= 1 and class_name.lower() == "total":
                # don't add TOTAL row if processing report with classes
                continue

            column_classes.append(class_name)

        # Determine report type once before processing rows
        is_profit_loss = False
        if hasattr(self, "path"):
            path = self.path().split("/")[-1]
            is_profit_loss = path == "ProfitAndLoss"

        # Process all accounts and return flat list
        accounts = []
        current_time = datetime.utcnow().isoformat() + "Z"
        self._process_rows(rows, accounts, start_period, end_period, currency, column_classes,
                          is_profit_loss=is_profit_loss, emitted_at=current_time)

        return accounts

    def _process_rows(self, rows: list, accounts: list, start_period: str, end_period: str, currency: str, column_classes: list,
                      parent_name: str = "", parent_id: str = "", grandparent_name: str = "", grandparent_id: str = "",
                      category_name: str = "", category_id: str = "", section_type: str = "",
                      is_profit_loss: bool = False, emitted_at: str = None):
        """Recursively process rows to extract account data"""

        for row in rows:
            row_type = row.get("type", "")
            account_id = ""
            if row_type == "Data":
                col_data = row.get("ColData", [])  # This is an account data row

                if len(col_data) >= 2:
                    account_name = col_data[0].get("value", "")
                    account_id = clean_id(col_data[0].get("id", ""))

                    if not account_id and account_name == "Net Income":
                        # Add hardcoded id for Net Income based on the old connector code
                        # https://github.com/Bainbridge-Growth/airbyte-singer-tap-quickbooks-airbyte-fork/blob/7a03f1c65c9451d0fd7482e2be345dcb71e12b87/tap_quickbooks/ReportProcessing.py
                        # Issue explained at https://linear.app/drivepoint/issue/ENG-2145/qbo-migration-net-income-from-balance-sheet-special-case
                        account_id = "1000930"

                    full_account_path = []

                    if category_name:
                        full_account_path.append(category_name)

                    if section_type and section_type != category_name and section_type not in full_account_path:
                        full_account_path.append(section_type)

                    if grandparent_name and grandparent_name != section_type and grandparent_name not in full_account_path:
                        full_account_path.append(grandparent_name)

                    if parent_name and parent_name not in full_account_path:
                        full_account_path.append(parent_name)

                    if account_name:
                        full_account_path.append(account_name)

                    full_account_name = ":".join(full_account_path)

                    self._create_account_records(accounts, col_data, account_name, account_id, start_period, end_period, currency,
                                                 parent_name, parent_id, grandparent_name, grandparent_id, category_name, category_id,
                                                 row, full_account_name, column_classes, emitted_at, is_profit_loss)

            elif row_type == "Section":
                # This is a section header - recurse into its rows
                section_name = row.get("group", "")
                header_col_data = row.get("Header", {}).get("ColData", [])
                section_display_name = ""
                if header_col_data:
                    section_display_name = header_col_data[0].get("value", "")

                section_id = clean_id(header_col_data[0].get("id", "") if header_col_data else "")

                nested_rows = row.get("Rows", {}).get("Row", [])

                # Handle special sections like "Current Assets", "Fixed Assets", etc.
                if not category_name:  # Top level (Assets, Liabilities, Equity)
                    new_category = section_display_name
                    new_category_id = ""
                    new_parent_name = ""
                    new_parent_id = ""
                    new_grandparent = ""
                    new_grandparent_id = ""
                    new_section_type = ""
                elif not parent_name:  # Second level (Current Assets, Fixed Assets, etc.)
                    new_category = category_name
                    new_category_id = category_id
                    new_parent_name = section_display_name
                    new_parent_id = section_id
                    new_grandparent = ""
                    new_grandparent_id = ""
                    new_section_type = section_display_name
                else:  # Third level and beyond
                    # Process differently based on report type
                    if is_profit_loss:
                        new_category, new_category_id, new_parent_name, new_parent_id, new_grandparent, new_grandparent_id, new_section_type = \
                            self._process_profit_loss_hierarchy(
                                category_name, category_id, section_display_name, section_id,
                                parent_name, parent_id, section_type
                            )
                    else:  # BalanceSheet or other reports
                        new_category, new_category_id, new_parent_name, new_parent_id, new_grandparent, new_grandparent_id, new_section_type = \
                            self._process_balance_sheet_hierarchy(
                                category_name, category_id, section_display_name, section_id,
                                parent_name, parent_id, grandparent_name, grandparent_id, section_type
                            )

                # Save the section header as a record if it has an ID (e.g., "4000 Sales of Product Income")
                # This ensures parent accounts are included in the output along with their children
                if section_id and header_col_data and len(header_col_data) >= 2:
                    # Build full account path for this header account
                    full_account_path = []
                    if category_name:
                        full_account_path.append(category_name)
                    if section_type and section_type != category_name and section_type not in full_account_path:
                        full_account_path.append(section_type)
                    if grandparent_name and grandparent_name != section_type and grandparent_name not in full_account_path:
                        full_account_path.append(grandparent_name)
                    if parent_name and parent_name not in full_account_path:
                        full_account_path.append(parent_name)
                    if section_display_name:
                        full_account_path.append(section_display_name)

                    full_account_name = ":".join(full_account_path)

                    # For header accounts, determine the correct parent based on hierarchy level
                    # If parent_name is empty, the header's parent is the category (e.g., Income, Cost of Goods Sold)
                    header_parent_name = parent_name if parent_name else category_name
                    header_parent_id = parent_id if parent_id else category_id
                    # For grandparent, use the category if we're at the second level
                    header_grandparent_name = grandparent_name if grandparent_name else category_name
                    header_grandparent_id = grandparent_id if grandparent_id else category_id

                    # Create records for this header account
                    self._create_account_records(
                        accounts, header_col_data, section_display_name, section_id,
                        start_period, end_period, currency,
                        header_parent_name, header_parent_id, header_grandparent_name, header_grandparent_id,
                        category_name, category_id,
                        row, full_account_name, column_classes, emitted_at, is_profit_loss
                    )

                # Process the section header as data if it has amounts
                # Note: Section headers are typically just for grouping/hierarchy and should not
                # be created as individual records. Only Data rows should become records.
                # The Summary rows contain totals but are not processed here.

                self._process_rows(
                    nested_rows, accounts, start_period, end_period, currency, column_classes,
                    new_parent_name, new_parent_id, new_grandparent, new_grandparent_id,
                    new_category, new_category_id, new_section_type,
                    is_profit_loss=is_profit_loss, emitted_at=emitted_at
                )

    def _process_profit_loss_hierarchy(self, category_name, category_id, section_display_name, section_id,
                                     parent_name, parent_id, section_type):
        """
        Handle P&L specific hierarchy rules
        """
        new_category = category_name
        new_category_id = category_id
        new_parent_name = section_display_name
        new_parent_id = section_id
        # In P&L reports, the category is the grandparent for third-level accounts
        # e.g. Income is the grandparent of "4005 Sales"
        new_grandparent = category_name
        new_grandparent_id = category_id
        new_section_type = section_type

        return new_category, new_category_id, new_parent_name, new_parent_id, new_grandparent, new_grandparent_id, new_section_type

    def _process_balance_sheet_hierarchy(self, category_name, category_id, section_display_name, section_id,
                                        parent_name, parent_id, grandparent_name, grandparent_id, section_type):
        """
        Handle Balance Sheet specific hierarchy
        """
        new_category = category_name
        new_category_id = category_id
        new_parent_name = section_display_name
        new_parent_id = section_id
        # For Balance Sheet, use the current parent as the grandparent for the next level
        # e.g. "Current Assets" is the grandparent of "Checking"
        new_grandparent = parent_name
        new_grandparent_id = parent_id
        new_section_type = section_type

        return new_category, new_category_id, new_parent_name, new_parent_id, new_grandparent, new_grandparent_id, new_section_type

    def get_json_schema(self) -> Mapping[str, Any]:
        return {
            "type": "object",
            "properties": {
                "_Account": {"type": ["null", "string"]},
                "_Account_id": {"type": ["null", "integer"]},
                "StartPeriod": {"type": ["null", "string"], "format": "date-time"},
                "EndPeriod": {"type": ["null", "string"], "format": "date-time"},
                "Currency": {"type": ["null", "string"]},
                "ParentAccountName": {"type": ["null", "string"]},
                "ParentAccountId": {"type": ["null", "integer"]},
                "GrandParentAccountName": {"type": ["null", "string"]},
                "GrandParentAccountId": {"type": ["null", "integer"]},
                "CategoryAccountName": {"type": ["null", "string"]},
                "CategoryAccountId": {"type": ["null", "integer"]},
                "Classification": {"type": ["null", "string"]},
                "FullyQualifiedName": {"type": ["null", "string"]},
                "AccountType": {"type": ["null", "string"]},
                "FullAccountName": {"type": ["null", "string"]},
                "Class": {"type": ["null", "string"]},
                "Dimension1": {"type": ["null", "string"]},
                "Total_Money": {"type": ["null", "number"]},
                "_airbyte_emitted_at": {"type": "string", "format": "date-time"}
            }
        }

    def _create_account_records(self, accounts, col_data, account_name, account_id, start_period, end_period, currency,
                                parent_name, parent_id, grandparent_name, grandparent_id, category_name, category_id,
                                row, full_account_name, column_classes, emitted_at, is_profit_loss):
        """Create account records for each column/class"""
        # Clean IDs once before the loop
        clean_parent_id = clean_id(parent_id)
        clean_grandparent_id = clean_id(grandparent_id)

        for i, class_name in enumerate(column_classes, 1):
            amount = ""
            if i < len(col_data):
                amount = col_data[i].get("value", "")

            # For balance sheet, grandparent is as set
            actual_grandparent_name = grandparent_name
            if is_profit_loss and parent_name and not grandparent_name:
                actual_grandparent_name = category_name

            account_record = {
                "_Account": account_name,
                "_Account_id": account_id,
                "StartPeriod": start_period,
                "EndPeriod": end_period,
                "Currency": currency,
                "ParentAccountName": parent_name,
                "ParentAccountId": clean_parent_id,
                "GrandParentAccountName": actual_grandparent_name,
                "GrandParentAccountId": clean_grandparent_id,
                "CategoryAccountName": category_name,
                "CategoryAccountId": category_id,
                "Classification": row.get("group", ""),
                "FullyQualifiedName": "",
                "AccountType": "",
                "FullAccountName": full_account_name,
                "Class": class_name,
                "Dimension1": self.current_dimension_name,
                "Total_Money": amount,
                "_airbyte_emitted_at": emitted_at
            }
            accounts.append(account_record)


class BalanceSheetReportMonthly(QuickbooksReportMonthlyBase):
    """QuickBooks Balance Sheet Report API connector

    Reference: https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/balancesheet
    """
    def path(
            self,
            stream_state: Mapping[str, Any] = None,
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> str:
        return f"company/{self.realm_id}/reports/BalanceSheet"


class ProfitLossReportMonthly(QuickbooksReportMonthlyBase):
    """QuickBooks Profit and Loss Report API connector

    Reference: https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/profitandloss
    """

    def path(
            self,
            stream_state: Mapping[str, Any] = None,
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> str:
        return f"company/{self.realm_id}/reports/ProfitAndLoss"
