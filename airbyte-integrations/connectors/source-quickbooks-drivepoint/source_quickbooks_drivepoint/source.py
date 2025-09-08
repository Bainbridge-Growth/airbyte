import requests

from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union
from source_quickbooks_drivepoint.streams import BalanceSheetReportStream
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http.requests_native_auth import Oauth2Authenticator, TokenAuthenticator


class SourceQuickbooksDrivepoint(AbstractSource):
    @staticmethod
    def get_authenticator(config):
        client_id = config.get("credentials")["client_id"]
        client_secret = config.get("credentials")["client_secret"]
        refresh_token = config.get("credentials")["refresh_token"]

        return Oauth2Authenticator(
            token_refresh_endpoint="https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
            client_id=client_id,
            client_secret=client_secret,
            refresh_token=refresh_token,
            grant_type="refresh_token",
        )

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            authenticator = self.get_authenticator(config)
            bs = BalanceSheetReportStream(
                realm_id=config["realm_id"],
                start_date=config.get("start_date"),
                end_date=config.get("end_date"),
                authenticator=authenticator
            )
            # TODO: test properly by requesting small BS report for last day

            return True, None
        except requests.exceptions.RequestException as e:
            if "401" in str(e):
                return False, "Authentication failed. Please verify your credentials."
            elif "403" in str(e):
                return False, "Authorization failed. Please ensure you have the correct permissions."
            return False, f"Unable to connect to YouTube Analytics API: {str(e)}"

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        authenticator = self.get_authenticator(config)
        streams = []
        streams.append(BalanceSheetReportStream(
            realm_id=config["realm_id"],
            start_date=config.get("start_date"),
            end_date=config.get("end_date"),
            authenticator=authenticator
        ))
        return streams

# TODO: Keep below for now, in case we need to override Oauth2Authenticator. Delete after testing
#
# class QuickbooksOauth2Authenticator(Oauth2Authenticator):
#     def __init__(self, config):
#         super().__init__(
#             token_refresh_endpoint="https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
#             client_id=config["client_id"],
#             client_secret=config["client_secret"],
#             refresh_token=config["refresh_token"],
#         )
#
#     def get_refresh_request_params(self) -> Mapping[str, Any]:
#         payload: MutableMapping[str, Any] = {
#             "grant_type": "refresh_token",
#             "client_id": self.get_client_id(),
#             "client_secret": self.get_client_secret(),
#             "refresh_token": self.get_refresh_token(),
#         }
#
#         return payload
#
#     def refresh_access_token(self) -> Tuple[str, int]:
#         try:
#             response = requests.request(
#                 method="POST",
#                 url=self.get_token_refresh_endpoint(),
#                 data=self.get_refresh_request_params(),  # Use 'data' instead of 'params'
#                 headers={"Content-Type": "application/x-www-form-urlencoded"}
#             )
#             response.raise_for_status()
#             response_json = response.json()
#             return response_json["access_token"], response_json["expires_in"]
#         except Exception as e:
#             raise Exception(f"Error while refreshing access token: {e}") from e


