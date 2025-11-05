import json
import os
import logging
import firebase_admin
import time
from typing import Optional, Dict, Any, List, Union
from firebase_admin import credentials, firestore

logger = logging.getLogger("airbyte")

class FirebaseClient:
    """
    A class for interacting with Firebase Firestore.
    Provides methods for reading and writing data to collections and documents.
    """

    def __init__(self, service_account_json: Union[str, Dict], project_id: str = None):
        """
        Initialize the Firebase client with service account credentials.

        Args:
            service_account_json: Either a path to a JSON file or a dictionary containing service account credentials
            project_id: Optional project ID if not specified in the credentials
        """
        self.app = None
        self.db = None
        self._initialize_firebase(service_account_json, project_id)

    def _initialize_firebase(self, service_account_json: Union[str, Dict], project_id: str = None):
        """
        Initialize the Firebase app and Firestore client.

        Args:
            service_account_json: Either a path to a JSON file or a dictionary containing service account credentials
            project_id: Optional project ID if not specified in the credentials
        """
        try:
            # Handle service account credentials provided as a file path
            if isinstance(service_account_json, str):
                if os.path.isfile(service_account_json):
                    cred = credentials.Certificate(service_account_json)
                else:
                    try:
                        cred_dict = json.loads(service_account_json)
                        cred = credentials.Certificate(cred_dict)
                    except json.JSONDecodeError:
                        raise ValueError("Service account JSON string is not valid JSON")
            else:
                # Handle service account credentials provided as a dictionary
                cred = credentials.Certificate(service_account_json)

            # Initialize the app with the provided credentials, but only if not already initialized
            app_options = {}
            if project_id:
                app_options['projectId'] = project_id

            try:
                self.app = firebase_admin.get_app()
            except ValueError:
                self.app = firebase_admin.initialize_app(cred, app_options)

            self.db = firestore.client()
        except Exception as e:
            raise ConnectionError(f"Failed to initialize Firebase connection: {str(e)}")

    def get_realm_id(self, company_id) -> Optional[str]:
        document = self._get_document('excelCompanies', company_id)
        if document and 'intuit' in document:
            return document['intuit'].get('realm_id')
        return None

    def get_refresh_token(self, company_id) -> Optional[str]:
        document = self._get_document('excelCompanies', company_id)
        if document and 'intuit' in document:
            return document['intuit'].get('token', {}).get('refresh_token')
        return None

    def update_token(self, company_id, token_json) -> bool:
        return self._update_document('excelCompanies', company_id, {
            'intuit.token': token_json
        })

    def update_refresh_info(self, company_id) -> bool:
        return self._update_document('excelCompanies', company_id, {
            'intuit.refresh_when': time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
            'intuit.refresh_who': 'source-quickbooks-drivepoint'
        })


    def _get_document(self, collection_name: str, document_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a document from a collection by its ID.

        Args:
            collection_name: The name of the collection
            document_id: The ID of the document

        Returns:
            The document data as a dictionary or None if not found
        """
        if not self.db:
            raise ConnectionError("Firebase not initialized")

        doc_ref = self.db.collection(collection_name).document(document_id)
        doc = doc_ref.get()

        if doc.exists:
            return doc.to_dict()
        return None


    def _update_document(self, collection_name: str, document_id: str, document_data: Dict[str, Any]) -> bool:
        """
        Update a document in a collection.

        Args:
            collection_name: The name of the collection
            document_id: The ID of the document to update
            document_data: The document data to update

        Returns:
            True if successful, False otherwise
        """
        if not self.db:
            raise ConnectionError("Firebase not initialized")

        doc_ref = self.db.collection(collection_name).document(document_id)
        doc = doc_ref.get()

        if not doc.exists:
            return False

        doc_ref.update(document_data)
        return True

    def close(self):
        """
        Clean up Firebase resources: https://firebase.google.com/docs/reference/admin/python/firebase_admin#firebase_admin.delete_app
        TODO: check if this is necessary or if firebase_admin handles it automatically.
        """
        if self.app:
            firebase_admin.delete_app(self.app)
            self.app = None
            self.db = None
