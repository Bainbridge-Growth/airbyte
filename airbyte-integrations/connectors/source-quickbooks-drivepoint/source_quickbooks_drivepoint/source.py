import requests
import logging
from datetime import datetime, timedelta
from typing import Any, List, Mapping, Tuple
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from source_quickbooks_drivepoint.auth_client import QuickbooksOauth2Authenticator
from source_quickbooks_drivepoint.report_streams import BalanceSheetReportMonthly, ProfitLossReportMonthly, TransactionListReportMonthly
from source_quickbooks_drivepoint.query_streams import Accounts, Bills, Classes, Customers, Departments, Employees, Items, JournalEntries, Invoices, Payments, PurchaseOrders, Purchases, Vendors

logger = logging.getLogger("airbyte")

class SourceQuickbooksDrivepoint(AbstractSource):
    @staticmethod
    def get_authenticator(config):
        # Handle both test config format (with 'credentials' nested) and production format
        credentials = config.get("credentials", {})

        return QuickbooksOauth2Authenticator(
            company_id=config.get("company_id") or config.get("realm_id"),  # Support both keys
            client_id=credentials.get("client_id") or config.get("client_id"),
            client_secret=credentials.get("client_secret") or config.get("client_secret"),
            refresh_token=credentials.get("refresh_token") or config.get("refresh_token")
        )

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            # Use a fixed small date range for connection test
            yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            today = datetime.now().strftime("%Y-%m-%d")

            authenticator = self.get_authenticator(config)
            realm_id = authenticator.firebase_client.get_realm_id(config.get("company_id"))

            bs = BalanceSheetReportMonthly(
                realm_id=realm_id,
                accounting_method=config.get("accounting_method", {}).get("selected_method", "Accrual"),
                first_dimension=config.get("balance_sheet_settings", {}).get("summarize_column", {}).get("selected_first_dimension"),
                second_dimension=config.get("balance_sheet_settings", {}).get("second_dimension", {}).get("selected_second_dimension"),
                start_date=yesterday,
                end_date=today,
                authenticator=authenticator
            )

            # Make actual API request for a small date range to verify connectivity
            logger.info("Testing connection by requesting balance sheet for the last day")
            records = list(bs.read_records(sync_mode=None))

            # If we get here without exceptions, the connection is working
            return True, None
        except requests.exceptions.RequestException as e:
            if "401" in str(e):
                return False, "Authentication failed. Please verify your credentials."
            elif "403" in str(e):
                return False, "Authorization failed. Please ensure you have the correct permissions."
            return False, f"Unable to connect to QuickBooks API: {str(e)}"
        except Exception as e:
            return False, f"Error testing connection to QuickBooks API: {str(e)}"

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        authenticator = self.get_authenticator(config)

        # Use realm_id from config if available (for tests), otherwise fetch from Firebase
        realm_id = config.get("realm_id")
        if not realm_id and hasattr(authenticator, 'firebase_client') and authenticator.firebase_client:
            realm_id = authenticator.firebase_client.get_realm_id(config.get("company_id") or config.get("realm_id"))

        streams = [
            Accounts(realm_id=realm_id, authenticator=authenticator),
            Bills(realm_id=realm_id, authenticator=authenticator),
            Classes(realm_id=realm_id, authenticator=authenticator),
            Customers(realm_id=realm_id, authenticator=authenticator),
            Departments(realm_id=realm_id, authenticator=authenticator),
            Employees(realm_id=realm_id, authenticator=authenticator),
            Items(realm_id=realm_id, authenticator=authenticator),
            JournalEntries(realm_id=realm_id, authenticator=authenticator),
            Invoices(realm_id=realm_id, authenticator=authenticator),
            Payments(realm_id=realm_id, authenticator=authenticator),
            PurchaseOrders(realm_id=realm_id, authenticator=authenticator),
            Purchases(realm_id=realm_id, authenticator=authenticator),
            Vendors(realm_id=realm_id, authenticator=authenticator)
        ]

        # Safely extract accounting method
        accounting_method = None
        if config.get("accounting_method"):
            accounting_method = config.get("accounting_method").get("selected_method")

        # Safely extract balance sheet settings
        bs_first_dimension = None
        bs_second_dimension = None
        if config.get("balance_sheet_settings"):
            if config.get("balance_sheet_settings").get("summarize_column"):
                bs_first_dimension = config.get("balance_sheet_settings").get("summarize_column").get("selected_first_dimension")
            if config.get("balance_sheet_settings").get("second_dimension"):
                bs_second_dimension = config.get("balance_sheet_settings").get("second_dimension").get("selected_second_dimension")

        # Safely extract profit loss settings
        pl_first_dimension = None
        pl_second_dimension = None
        if config.get("profit_loss_settings"):
            if config.get("profit_loss_settings").get("summarize_column"):
                pl_first_dimension = config.get("profit_loss_settings").get("summarize_column").get("selected_first_dimension")
            if config.get("profit_loss_settings").get("second_dimension"):
                pl_second_dimension = config.get("profit_loss_settings").get("second_dimension").get("selected_second_dimension")

        streams.extend([
            BalanceSheetReportMonthly(
                realm_id=realm_id,
                accounting_method=accounting_method if accounting_method else "Accrual",
                first_dimension=bs_first_dimension,
                second_dimension=bs_second_dimension,
                start_date=config.get("start_date"),
                end_date=config.get("end_date"),
                authenticator=authenticator
            ),
            ProfitLossReportMonthly(
                realm_id=realm_id,
                accounting_method=accounting_method if accounting_method else "Accrual",
                first_dimension=pl_first_dimension,
                second_dimension=pl_second_dimension,
                start_date=config.get("start_date"),
                end_date=config.get("end_date"),
                authenticator=authenticator
            ),
            TransactionListReportMonthly(
                realm_id=realm_id,
                start_date=config.get("start_date"),
                end_date=config.get("end_date"),
                authenticator=authenticator
            )
        ])

        return streams

