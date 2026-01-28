import pendulum
import requests
import logging
from datetime import datetime
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.models import AirbyteStateMessage, SyncMode
from .query_streams import Classes, Departments, Customers, Vendors

logger = logging.getLogger("airbyte")


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


class QuickbooksReportMonthlyBase(HttpStream):
    """Base class for QuickBooks Reports API connectors

    Reference: https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities
    """

    primary_key = ["_Account_id", "Class", "StartPeriod"]
    url_base = "https://quickbooks.api.intuit.com/v3/"

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
        super().__init__(authenticator=authenticator, **kwargs)

    def _get_query_stream_class(self):
        mapping = {
            "Classes": Classes,
            "Departments": Departments,
            "Customers": Customers,
            "Vendors": Vendors
        }
        return mapping.get(self.second_dimension)

    def _get_dimension_param_name(self):
        mapping = {
            "Classes": "class",
            "Departments": "department",
            "Customers": "customer",
            "Vendors": "vendor"
        }
        return mapping.get(self.second_dimension)

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

        # Only add summarize_column_by if first_dimension is set and not None
        if self.first_dimension is not None:
            params["summarize_column_by"] = self.first_dimension

        # When second_dimension is set, use first_dimension as summarize_column_by
        # and filter by the current second_dimension item
        if self.second_dimension and self.current_dimension_id:
            # Filter by second_dimension item
            param_name = self._get_dimension_param_name()
            if param_name:
                params[param_name] = self.current_dimension_id
                self.logger.info(f"Using {self.first_dimension} as summarize_column_by and filtering by {param_name}={self.current_dimension_id}")

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

    def read_records(self, sync_mode, cursor_field=None, stream_slice=None, stream_state=None):
        # When second_dimension is set AND we haven't fetched dimension items yet,
        # we fetch dimension items and manage slicing ourselves.
        # We detect the initial call by checking if current_dimension_id is None.
        # Once we set current_dimension_id and call super().read_records(), subsequent
        # nested calls will have current_dimension_id set and will use normal flow.
        if self.second_dimension and self.current_dimension_id is None:
            # When second_dimension is provided, we need to handle slicing ourselves
            # This is the initial call - we haven't started processing dimensions yet

            # Get all stream slices for the entire period
            all_slices = list(self.stream_slices(sync_mode, cursor_field, stream_state))

            # Fetch the dimension items once for the entire period
            query_stream_class = self._get_query_stream_class()
            if not query_stream_class:
                self.logger.error(f"Unknown second_dimension: {self.second_dimension}")
                return

            query_stream = query_stream_class(
                realm_id=self.realm_id,
                start_date=self.start_date,
                end_date=self.end_date,
                authenticator=self.authenticator
            )

            # Fetch all second_dimension items and extract distinct Id->Name pairs
            second_dimension_items = list(query_stream.read_records(sync_mode=None))
            self.logger.info(f"Fetched {len(second_dimension_items)} items for {self.second_dimension}")

            # Extract distinct Id->Name pairs
            distinct_items = {}
            for item in second_dimension_items:
                item_id = item.get("Id")
                item_name = item.get("Name")
                if item_id and item_name:
                    distinct_items[item_id] = item_name

            self.logger.info(f"Found {len(distinct_items)} distinct Id->Name pairs for {self.second_dimension}")

            # Get the parameter name for this dimension type (class/department/customer/vendor)
            param_name = self._get_dimension_param_name()

            # For each time slice, first fetch the total (without dimension filter)
            # then fetch data for each dimension item
            for time_slice in all_slices:
                # First, fetch report without second_dimension filter to get totals
                self.logger.info(f"Fetching TOTAL report (no {param_name} filter) for period {time_slice['start_date']} to {time_slice['end_date']}")

                # Set dimension info to indicate this is the total
                self.current_dimension_id = None  # No filter applied
                self.current_dimension_name = "DRIVEPOINT_CLASS_TOTAL"

                # Fetch records without dimension filter
                yield from super().read_records(sync_mode, cursor_field, time_slice, stream_state)

                # Now fetch for each distinct Id->Name pair with dimension filter
                for item_id, item_name in distinct_items.items():
                    self.logger.info(f"Fetching report for {param_name}={item_id} (Name: {item_name}) for period {time_slice['start_date']} to {time_slice['end_date']}")

                    # Set current dimension info for use in request_params and _create_account_records
                    self.current_dimension_id = item_id
                    self.current_dimension_name = item_name

                    # Fetch records for this time slice with this dimension filter
                    yield from super().read_records(sync_mode, cursor_field, time_slice, stream_state)
        else:
            # Normal flow: no second_dimension OR we're in a nested call with current_dimension_id already set
            if not self.second_dimension:
                # Only clear dimension info if second_dimension is not configured
                self.current_dimension_id = None
                self.current_dimension_name = None
            yield from super().read_records(sync_mode, cursor_field, stream_slice, stream_state)

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
        response_json = response.json()
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
            class_name = col_title.replace(" ", "").replace("-", "") if col_title else f"Column_{i}"

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
