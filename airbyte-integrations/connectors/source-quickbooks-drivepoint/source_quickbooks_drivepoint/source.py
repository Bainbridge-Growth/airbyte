import requests
import time
from datetime import datetime, timedelta
from typing import Any, List, Mapping, Tuple, MutableMapping
from source_quickbooks_drivepoint.streams import BalanceSheetReportMonthly, ProfitAndLossReportMonthly
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http.requests_native_auth import Oauth2Authenticator, TokenAuthenticator


class SourceQuickbooksDrivepoint(AbstractSource):
    @staticmethod
    def get_authenticator(config):
        return QuickbooksOauth2Authenticator(
            client_id=config.get("credentials")["client_id"],
            client_secret=config.get("credentials")["client_secret"],
            refresh_token=config.get("credentials")["refresh_token"]
        )

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            # Get yesterday's date in YYYY-MM-DD format
            today = datetime.now().strftime("%Y-%m-%d")
            yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

            authenticator = self.get_authenticator(config)
            bs = BalanceSheetReportMonthly(
                realm_id=config.get("realm_id"),
                start_date=yesterday,  # Use yesterday
                end_date=today,  # Use today
                authenticator=authenticator
            )

            # Make actual API request for a small date range to verify connectivity
            logger.info("Testing connection by requesting balance sheet for last day")
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
        streams = [
            BalanceSheetReportMonthly(
                realm_id=config["realm_id"],
                start_date=config.get("start_date"),
                end_date=config.get("end_date"),
                authenticator=authenticator
            ),
            ProfitAndLossReportMonthly(
                realm_id=config["realm_id"],
                start_date=config.get("start_date"),
                end_date=config.get("end_date"),
                authenticator=authenticator
            )
        ]
        return streams


class QuickbooksOauth2Authenticator(Oauth2Authenticator):
    def __init__(self, client_id, client_secret, refresh_token):
        self.refresh_token = refresh_token
        super().__init__(
            token_refresh_endpoint="https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
            client_id=client_id,
            client_secret=client_secret,
            refresh_token=refresh_token,
            grant_type="refresh_token",
        )
        # Initialize these to prevent token expiry errors
        self.access_token = None
        self.token_expiry_date = None

    def token_has_expired(self) -> bool:
        """Override token_has_expired to handle QuickBooks specific logic"""
        if self.token_expiry_date is None:
            return True  # If we don't have an expiry date, assume the token expired

        current_time = int(time.time())
        return current_time >= self.token_expiry_date

    def refresh_access_token(self) -> Tuple[str, int]:
        try:
            form_data = {
                "grant_type": "refresh_token",
                "refresh_token": self.get_refresh_token(),
                "client_id": self.get_client_id(),
                "client_secret": self.get_client_secret()
            }

            response = requests.post(
                self.get_token_refresh_endpoint(),
                data=form_data,
                headers={"Content-Type": "application/x-www-form-urlencoded"}
            )

            if response.status_code != 200:
                print(f"Token refresh failed: Status {response.status_code}, Response: {response.text}")
                response.raise_for_status()

            response_json = response.json()

            # Store new refresh token if provided in response
            if "refresh_token" in response_json:
                self.refresh_token = response_json["refresh_token"]

            # Calculate token expiry time
            self.token_expiry_date = int(time.time()) + response_json["expires_in"]

            return response_json["access_token"], response_json["expires_in"]
        except Exception as e:
            raise Exception(f"Error while refreshing access token: {e}") from e
