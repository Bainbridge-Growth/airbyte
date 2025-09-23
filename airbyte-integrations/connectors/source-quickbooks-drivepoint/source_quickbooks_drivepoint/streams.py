import pendulum
import requests
from datetime import datetime
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.models import AirbyteStateMessage, SyncMode


class QuickbooksReportMonthlyBase(HttpStream):
    """Base class for QuickBooks Reports API connectors

    Reference: https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities
    """

    primary_key = ["_Account_id", "Class", "StartPeriod"]
    url_base = "https://quickbooks.api.intuit.com/v3/"

    def __init__(
            self,
            realm_id: str,  # company id
            start_date: str = None,
            end_date: str = None,
            **kwargs
    ):
        self.realm_id = realm_id
        self.start_date = start_date
        self.end_date = end_date
        super().__init__(**kwargs)

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
            start_dt = datetime.strptime(self.start_date, "%Y-%m-%d")
        else:
            start_dt = self.start_date

        if self.end_date:
            if isinstance(self.end_date, str):
                end_dt = datetime.strptime(self.end_date, "%Y-%m-%d")
            else:
                end_dt = self.end_date
        else:
            end_dt = datetime.now()

        # Create new pendulum dates directly
        start = pendulum.datetime(start_dt.year, start_dt.month, start_dt.day)
        end = pendulum.datetime(end_dt.year, end_dt.month, end_dt.day)

        # Log the full period we're processing
        self.logger.info(f"Breaking period {start.format('YYYY-MM-DD')} to {end.format('YYYY-MM-DD')} into monthly chunks")

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
            "accounting_method": "Accrual",
            "minorversion": 70,

            # TODO: these are possible summarize_column_by values - decide which to use
            "summarize_column_by": "Classes"
            #"summarize_column_by": "Total"
            #"summarize_column_by": "Month"
        }
        self.logger.info(f"Processing slice with dates: {stream_slice}")

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
        # Call the parent implementation which will use _send_request
        records_generator = super().read_records(
            sync_mode=sync_mode,
            cursor_field=cursor_field,
            stream_slice=stream_slice,
            stream_state=stream_state
        )

        # Yield all records from the parent implementation
        for record in records_generator:
            yield record

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

        start_period = header.get("StartPeriod")
        end_period = header.get("EndPeriod")
        currency = header.get("Currency")

        self.logger.info(f"API returned data for period: {start_period} to {end_period}")

        # Build column mapping (skip first column which is account name)
        column_classes = []
        for i, col in enumerate(columns[1:], 1):  # Skip first column (Account)
            col_title = col.get("ColTitle", "")
            class_name = col_title.replace(" ", "").replace("-", "") if col_title else f"Column_{i}"

            if len(column_classes) > 1 and class_name.lower() == "total":
                # don't add TOTAL row if processing report with classes
                continue

            column_classes.append(class_name)

        # Process all accounts and return flat list
        accounts = []
        self._process_rows(rows, accounts, start_period, end_period, currency, column_classes)

        return accounts

    def _process_rows(self, rows: list, accounts: list, start_period: str, end_period: str, currency: str, column_classes: list,
                      parent_name: str = "", parent_id: str = "", grandparent_name: str = "", grandparent_id: str = "",
                      category_name: str = "", category_id: str = "", section_type: str = ""):
        """Recursively process rows to extract account data"""

        # Determine if this is a P&L report or Balance Sheet report based on the path or report structure
        is_profit_loss = False
        if hasattr(self, "path"):
            path = self.path().split("/")[-1]
            is_profit_loss = path == "ProfitAndLoss"

        for row in rows:
            row_type = row.get("type", "")
            account_id = ""
            if row_type == "Data":
                col_data = row.get("ColData", [])  # This is an account data row

                if len(col_data) >= 2:
                    account_name = col_data[0].get("value", "")
                    account_id = col_data[0].get("id", "")
                    if account_id and " at index " in account_id:
                        account_id = account_id.split(" at index ")[0]

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

                    # Create one record for each column/class (skip first column which is account name)
                    for i, class_name in enumerate(column_classes, 1):
                        amount = ""
                        if i < len(col_data):
                            amount = col_data[i].get("value", "")

                        clean_parent_id = parent_id
                        if clean_parent_id and " at index " in clean_parent_id:
                            clean_parent_id = clean_parent_id.split(" at index ")[0]

                        clean_grandparent_id = grandparent_id
                        if clean_grandparent_id and " at index " in clean_grandparent_id:
                            clean_grandparent_id = clean_grandparent_id.split(" at index ")[0]

                        # For P&L reports, if we're at a third level entry with a parent but no grandparent,
                        # use the category as the grandparent (e.g. Income is the grandparent of "4005 Sales")
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
                            "Total_Money": amount
                        }
                        accounts.append(account_record)

            elif row_type == "Section":
                # This is a section header - recurse into its rows
                section_name = row.get("group", "")
                header_col_data = row.get("Header", {}).get("ColData", [])
                section_display_name = ""
                if header_col_data:
                    section_display_name = header_col_data[0].get("value", "")

                section_id = header_col_data[0].get("id", "") if header_col_data else ""
                if section_id and " at index " in section_id:
                    section_id = section_id.split(" at index ")[0]

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

                self._process_rows(
                    nested_rows, accounts, start_period, end_period, currency, column_classes,
                    new_parent_name, new_parent_id, new_grandparent, new_grandparent_id,
                    new_category, new_category_id, new_section_type
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
        """Define the schema for account data"""
        return {
            "type": "object",
            "properties": {
                "_Account": {"type": "string"},
                "_Account_id": {"type": "string"},
                "StartPeriod": {"type": "string"},
                "EndPeriod": {"type": "string"},
                "Currency": {"type": "string"},
                "ParentAccountName": {"type": "string"},
                "ParentAccountId": {"type": "string"},
                "GrandParentAccountName": {"type": "string"},
                "GrandParentAccountId": {"type": "string"},
                "CategoryAccountName": {"type": "string"},
                "CategoryAccountId": {"type": "string"},
                "Classification": {"type": "string"},
                "FullyQualifiedName": {"type": "string"},
                "AccountType": {"type": "string"},
                "FullAccountName": {"type": "string"},
                "Class": {"type": "string"},
                "Total_Money": {"type": "string"}
            }
        }

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

class ProfitAndLossReportMonthly(QuickbooksReportMonthlyBase):
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
