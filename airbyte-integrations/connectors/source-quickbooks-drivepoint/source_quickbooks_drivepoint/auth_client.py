import requests
import os
import time
import logging
import threading
from typing import Tuple, Union, Mapping, Any
from airbyte_cdk.sources.declarative.interpolation import InterpolatedString
from airbyte_cdk.sources.streams.http.requests_native_auth import Oauth2Authenticator, TokenAuthenticator
from source_quickbooks_drivepoint.firebase_client import FirebaseClient
from source_quickbooks_drivepoint.secret_manager_client import SecretManagerClient

logger = logging.getLogger("airbyte")

class QuickbooksOauth2Authenticator(Oauth2Authenticator):
    firebase_client = None

    """
    Custom implementation of Oauth2Authenticator that allows to refresh QuickBooks access tokens using refresh tokens stored in Firebase
    """
    def __init__(self, company_id, client_id, client_secret, refresh_token=None):
        # Initialize these to prevent token expiry errors
        self.access_token = None
        self.token_expiry_date = None

        self.company_id = company_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token

        firebase_project_id = "exceladdinprod"

        if not self.refresh_token:
            logger.debug(f"No refresh token provided, attempting to fetch from Firebase for company_id: {self.company_id}")

            if os.path.exists('secrets/firebase_service_account.json'):
                self.firebase_client = FirebaseClient('secrets/firebase_service_account.json', firebase_project_id)
            else:
                secrets_manager = SecretManagerClient("data-infrastructure-324613")
                firebase_sa = secrets_manager.get_firebase_service_account()
                self.firebase_client = FirebaseClient(firebase_sa, firebase_project_id)

            self.refresh_token = self.firebase_client.get_refresh_token(self.company_id)

            if not self.refresh_token:
                logger.error(f"No refresh token found in Firebase for company_id {self.company_id}")

        super().__init__(
            token_refresh_endpoint="https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
            client_id=client_id,
            client_secret=client_secret,
            refresh_token=self.refresh_token,
            grant_type="refresh_token",
        )

    def get_refresh_token(self) -> str:
        if not self.refresh_token:
            self.refresh_token = self.firebase_client.get_refresh_token(self.company_id)
        return self.refresh_token

    def token_has_expired(self) -> bool:
        """Override token_has_expired to handle QuickBooks specific logic"""
        if self.token_expiry_date is None:
            return True

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
                logger.error(f"Token refresh failed: Status {response.status_code}, Response: {response.text}")
                response.raise_for_status()

            response_json = response.json()

            if "refresh_token" in response_json:
                logger.info("Received a refresh token from QuickBooks API. Updating it in Firebase.")
                self.firebase_client.update_token(self.company_id, response_json)
                self.firebase_client.update_refresh_info(self.company_id)
                self.refresh_token = response_json["refresh_token"]
                self.token_expiry_date = int(time.time()) + response_json["expires_in"]

            return response_json["access_token"], response_json["expires_in"]
        except Exception as e:
            raise Exception(f"Error while refreshing access token: {e}") from e
