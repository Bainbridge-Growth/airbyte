import json
import logging
from typing import Dict, Any, Optional

from google.cloud import secretmanager


class SecretManagerClient:

    def __init__(self, project_id: str, logger: Optional[logging.Logger] = None):
        """
        Initialize the Secret Manager client.

        Args:
            project_id: GCP project ID
            logger: Logger instance (optional)
        """
        self.project_id = project_id
        self.logger = logger or logging.getLogger(__name__)
        try:
            self.client = secretmanager.SecretManagerServiceClient()
            self.logger.info(f"Secret Manager client initialized for project {project_id}")
        except Exception as e:
            self.logger.error(f"Failed to initialize Secret Manager client: {e}")
            raise

    def get_secret(self, secret_name: str, version: str = "latest") -> str:
        """
        Retrieve a secret from Secret Manager.

        Args:
            secret_name: Name of the secret in Secret Manager
            version: Version of the secret (default: "latest")

        Returns:
            Secret value as a string
        """
        try:
            # Build the resource name of the secret version
            name = f"projects/{self.project_id}/secrets/{secret_name}/versions/{version}"

            # Access the secret version
            self.logger.info(f"Retrieving secret {secret_name} (version: {version})")
            response = self.client.access_secret_version(request={"name": name})

            # Return the decoded payload
            return response.payload.data.decode("UTF-8")
        except Exception as e:
            self.logger.error(f"Failed to retrieve secret {secret_name}: {e}")
            raise

    def get_json_secret(self, secret_name: str, version: str = "latest") -> Dict[str, Any]:
        """
        Retrieve a JSON secret from Secret Manager and parse it.

        Args:
            secret_name: Name of the secret in Secret Manager
            version: Version of the secret (default: "latest")

        Returns:
            Parsed JSON as a dictionary
        """
        try:
            secret_value = self.get_secret(secret_name, version)
            return json.loads(secret_value)
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse JSON secret {secret_name}: {e}")
            raise ValueError(f"Secret {secret_name} is not valid JSON: {e}")

    def get_firebase_service_account(self) -> Dict[str, Any]:
        """
        Retrieve Firebase service account credentials from Secret Manager.

        Args:
            secret_name: Name of the secret containing Firebase service account JSON (default: "firebase-service-account")

        Returns:
            Firebase service account credentials as a dictionary
        """
        return self.get_json_secret("FIREBASE_CONFIG_PRODUCTION")
