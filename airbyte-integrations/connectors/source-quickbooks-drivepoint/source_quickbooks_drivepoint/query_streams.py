import os
import json
import requests
import logging
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional
from airbyte_cdk.sources.streams.http import HttpStream
from datetime import datetime

logger = logging.getLogger("airbyte")

MAX_RESULTS_PER_PAGE = 1000  # QuickBooks API maximum results per page

class QueryStreamBase(HttpStream):
    """
    Stream for QuickBooks /query endpoint
    """

    # Define primary key as specified in manifest
    primary_key = "Id"

    def __init__(
        self,
        realm_id: str,
        start_date: str = None,
        end_date: str = None,
        authenticator=None,
        **kwargs
    ):
        self.realm_id = realm_id
        self.start_date = start_date
        self.end_date = end_date
        self.current_token = None
        super().__init__(authenticator=authenticator)

    @property
    def url_base(self) -> str:
        return "https://quickbooks.api.intuit.com/v3/"

    @property
    def entity_name(self) -> str:
        """
        Return stream name matching schema filename
        """
        return self.__class__.__name__

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> str:
        return f"company/{self.realm_id}/query"

    def request_headers(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Mapping[str, Any]:
        """
        QuickBooks API requires these specific headers
        """
        return {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        """
        Format the query parameters for QuickBooks API
        """
        try:
            start_time = stream_slice.get("start_time") if stream_slice else "1970-01-01T00:00:00Z"
            end_time = stream_slice.get("end_time") if stream_slice else datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
            start_position = (next_page_token.get("next_page_token", 0) if next_page_token else 0)
            self.current_token = next_page_token

            # Build query exactly matching the manifest format
            query = f"""SELECT * FROM {self.entity_name}
                WHERE Metadata.LastUpdatedTime > '{start_time}'
                AND Metadata.LastUpdatedTime <= '{end_time}'
                AND Active IN (true, false)
                ORDER BY Metadata.LastUpdatedTime ASC
                STARTPOSITION {start_position}
                MAXRESULTS {MAX_RESULTS_PER_PAGE}"""

            logger.debug(f"Built query for {self.entity_name}: start_time={start_time}, end_time={end_time}, start_position={start_position}")
            return {"query": query}
        except Exception as e:
            logger.error(f"Error building query parameters: {str(e)}")
            raise

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        Parse the response from QuickBooks API with detailed logging
        """
        try:
            try:
                json_response = response.json()
            except Exception as e:
                logger.error(f"Failed to parse response as JSON: {str(e)}")
                logger.error(f"Response content: {response.text}")
                return []

            if "Fault" in json_response:
                logger.error(f"QuickBooks API error: {json_response.get('Fault')}")
                return []

            records = json_response.get("QueryResponse", {}).get(self.entity_name, [])
            logger.info(f"Received {len(records)} records for {self.entity_name}")

            current_time = datetime.utcnow().isoformat()
            for record in records:
                record["_airbyte_emitted_at"] = current_time
                yield record

        except Exception as e:
            logger.error(f"Error parsing response: {str(e)}")
            logger.error(f"Response content: {response.text}")
            raise

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        Handle pagination using the number of records returned.
        """
        try:
            json_response = response.json()
            records = json_response.get("QueryResponse", {}).get(self.entity_name, [])
            num_records = len(records)

            # If we got MAX_RESULTS_PER_PAGE records, there may be more pages
            if num_records == MAX_RESULTS_PER_PAGE:
                # Use the stored current token
                current_start = (self.current_token.get("next_page_token", 0) if self.current_token else 0)
                next_start = current_start + MAX_RESULTS_PER_PAGE
                next_token = {"next_page_token": next_start}
                return next_token
            return None
        except Exception as e:
            logger.error(f"Error calculating next page token: {str(e)}")
            return None

    def stream_slices(
        self,
        sync_mode: str,
        cursor_field: List[str] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        """
        Create stream slices with proper date formatting
        """
        try:
            # Match the format exactly as shown in the manifest
            if self.start_date and "T" not in self.start_date:
                start_time = f"{self.start_date}T00:00:00Z"
            else:
                start_time = self.start_date or "1970-01-01T00:00:00Z"

            if self.end_date and "T" not in self.end_date:
                end_time = f"{self.end_date}T00:00:00Z"
            else:
                end_time = self.end_date or datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

            logger.debug(f"Created stream slice from {start_time} to {end_time}")
            return [{"start_time": start_time, "end_time": end_time}]
        except Exception as e:
            logger.error(f"Error creating stream slices: {str(e)}")
            raise

    def _send_request(self, request: requests.PreparedRequest, request_kwargs: Mapping[str, Any]) -> requests.Response:
        """
        Override _send_request to add request logging
        """
        response = self._session.send(request, **request_kwargs)

        logger.info(f"Response status: {response.status_code}")

        if response.status_code != 200:
            logger.error(f"Error response: {response.text}")

        return response


class Accounts(QueryStreamBase):
    @property
    def name(self) -> str:
        return self.__class__.__name__.lower()

    @property
    def entity_name(self) -> str:
        return "Account"


class Classes(QueryStreamBase):
    @property
    def name(self) -> str:
        return self.__class__.__name__.lower()

    @property
    def entity_name(self) -> str:
        return "Class"


class Customers(QueryStreamBase):
    @property
    def name(self) -> str:
        return self.__class__.__name__.lower()

    @property
    def entity_name(self) -> str:
        return "Customer"


class Departments(QueryStreamBase):
    @property
    def name(self) -> str:
        return self.__class__.__name__.lower()

    @property
    def entity_name(self) -> str:
        return "Department"


class Vendors(QueryStreamBase):
    @property
    def name(self) -> str:
        return self.__class__.__name__.lower()

    @property
    def entity_name(self) -> str:
        return "Vendor"
