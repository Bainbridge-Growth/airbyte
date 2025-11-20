import requests
import logging
from datetime import datetime, timedelta
from typing import Any, List, Mapping, Tuple
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from source_quickbooks_drivepoint.auth_client import QuickbooksOauth2Authenticator
from source_quickbooks_drivepoint.report_streams import BalanceSheetReportMonthly, ProfitLossReportMonthly
from source_quickbooks_drivepoint.query_streams import Accounts, Classes, Customers, Departments, Vendors

logger = logging.getLogger("airbyte")

class SourceQuickbooksDrivepoint(AbstractSource):
    @staticmethod
    def get_authenticator(config):
        return QuickbooksOauth2Authenticator(
            company_id=config.get("company_id"),
            client_id=config.get("client_id"),
            client_secret=config.get("client_secret"),
            refresh_token=config.get("refresh_token")
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
        realm_id = authenticator.firebase_client.get_realm_id(config.get("company_id"))

        streams = [
            Accounts(realm_id=realm_id, start_date=config.get("start_date"), end_date=config.get("end_date"), authenticator=authenticator),
            Classes(realm_id=realm_id, start_date=config.get("start_date"), end_date=config.get("end_date"), authenticator=authenticator),
            Customers(realm_id=realm_id, start_date=config.get("start_date"), end_date=config.get("end_date"), authenticator=authenticator),
            Departments(realm_id=realm_id, start_date=config.get("start_date"), end_date=config.get("end_date"), authenticator=authenticator),
            Vendors(realm_id=realm_id, start_date=config.get("start_date"), end_date=config.get("end_date"), authenticator=authenticator)
        ]

        accounting_method = config.get("accounting_method").get("selected_method") if config.get("accounting_method") else None

        streams.extend([
            BalanceSheetReportMonthly(
                realm_id=realm_id,
                accounting_method=accounting_method,
                first_dimension=config.get("balance_sheet_settings").get("summarize_column").get("selected_first_dimension") if config.get("balance_sheet_settings").get("summarize_column") else None,
                second_dimension=config.get("balance_sheet_settings").get("second_dimension").get("selected_second_dimension") if config.get("balance_sheet_settings").get("second_dimension") else None,
                start_date=config.get("start_date"),
                end_date=config.get("end_date"),
                authenticator=authenticator
            ),
            ProfitLossReportMonthly(
                realm_id=realm_id,
                accounting_method=accounting_method,
                first_dimension=config.get("profit_loss_settings").get("summarize_column").get("selected_first_dimension") if config.get("profit_loss_settings").get("summarize_column") else None,
                second_dimension=config.get("profit_loss_settings").get("second_dimension").get("selected_second_dimension") if config.get("profit_loss_settings").get("second_dimension") else None,
                start_date=config.get("start_date"),
                end_date=config.get("end_date"),
                authenticator=authenticator
            )
        ])

        # base_streams = super().streams(config) or []
        # for stream in base_streams:
        #     if hasattr(stream, "authenticator"):
        #         stream.authenticator = authenticator
        # streams.extend(base_streams)

        return streams

