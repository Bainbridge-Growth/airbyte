from typing import Any, Iterable, List, Mapping, MutableMapping, Optional
import requests
from airbyte_cdk.sources.streams.http import HttpStream


class BalanceSheetReportStream(HttpStream):
    """QuickBooks Balance Sheet Report API connector

    Reference: https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/balancesheet
    """

    primary_key = "id"
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

    def path(
            self,
            stream_state: Mapping[str, Any] = None,
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> str:
        return f"company/{self.realm_id}/reports/BalanceSheet"

    def request_params(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        params = {"summarize_column_by": "Total"}

        if self.start_date:
            params["start_date"] = self.start_date

        if self.end_date:
            params["end_date"] = self.end_date

        return params

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

        # Extract common report info
        start_period = header.get("StartPeriod")
        end_period = header.get("EndPeriod")
        currency = header.get("Currency")

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
                      category_name: str = "", category_id: str = ""):
        """Recursively process rows to extract account data"""

        for row in rows:
            row_type = row.get("type", "")

            if row_type == "Data":
                # This is an account data row
                col_data = row.get("ColData", [])
                if len(col_data) >= 2:
                    account_name = col_data[0].get("value", "")
                    account_id = col_data[0].get("id", "")

                    # Build the full account hierarchy
                    full_name_parts = [part for part in [category_name, grandparent_name, parent_name, account_name] if part]
                    full_account_name = ":".join(full_name_parts)

                    # Create one record for each column/class (skip first column which is account name)
                    for i, class_name in enumerate(column_classes, 1):  # Start from index 1 (skip account column)
                        amount = ""
                        if i < len(col_data):
                            amount = col_data[i].get("value", "")

                        account_record = {
                            "_Account": account_name,
                            "_Account_id": account_id,
                            "StartPeriod": start_period,
                            "EndPeriod": end_period,
                            "Currency": currency,
                            "ParentAccountName": parent_name,
                            "ParentAccountId": parent_id,
                            "GrandParentAccountName": grandparent_name,
                            "GrandParentAccountId": grandparent_id,
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

                nested_rows = row.get("Rows", {}).get("Row", [])

                if not category_name:  # Top level (Assets, Liabilities, Equity)
                    new_category = section_display_name
                    new_category_id = ""
                    new_parent = ""
                    new_parent_id = ""
                    new_grandparent = ""
                    new_grandparent_id = ""
                elif not parent_name:  # Second level (Current Assets, Fixed Assets, etc.)
                    new_category = category_name
                    new_category_id = category_id
                    new_parent = section_display_name
                    new_parent_id = parent_id
                    new_grandparent = ""
                    new_grandparent_id = parent_id
                else:  # Third level and beyond
                    new_category = category_name
                    new_category_id = category_id
                    new_parent = section_display_name
                    new_parent_id = parent_id
                    new_grandparent = parent_name
                    new_grandparent_id = parent_id

                self._process_rows(
                    nested_rows, accounts, start_period, end_period, currency, column_classes,
                    new_parent, new_parent_id, new_grandparent, new_grandparent_id,
                    new_category, new_category_id
                )

    def get_json_schema(self) -> Mapping[str, Any]:
        """Define the schema for balance sheet account data"""
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
