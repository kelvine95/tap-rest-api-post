# tap_rest_api_post/streams.py
"""Stream class for tap-rest-api-post."""

import json
from typing import Any, Dict, Iterable, Optional

from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.helpers._util import classproperty
from singer_sdk.streams import RESTStream

from tap_rest_api_post.pagination import TotalPagesPaginator


class DynamicStream(RESTStream):
    """
    A dynamic REST stream driven entirely by its configuration.
    """
    def __init__(self, tap, name: str, config: Dict[str, Any]):
        """Initialize the dynamic stream."""
        self.stream_config = config
        # The parent constructor will use the `name` and `schema` properties below.
        super().__init__(tap=tap)

    @property
    def name(self) -> str:
        """Return the stream's name."""
        return self.stream_config["name"]

    @property
    def primary_keys(self) -> Optional[List[str]]:
        """Return the list of primary key fields."""
        return self.stream_config.get("primary_keys")

    @property
    def replication_key(self) -> Optional[str]:
        """Return the replication key field."""
        return self.stream_config.get("replication_key")

    @property
    def url_base(self) -> str:
        """Return the API URL base."""
        return self.stream_config["api_url"]

    @property
    def path(self) -> str:
        """Return the API endpoint path."""
        return f'/{self.stream_config["path"].lstrip("/")}'

    @property
    def http_method(self) -> str:
        """Return the HTTP method, which is always POST for this tap."""
        return "POST"

    # Use the SDK's authenticator property for clean authentication handling.
    @classproperty
    def authenticator(self) -> APIKeyAuthenticator:
        """Return a new authenticator instance."""
        header = self.stream_config.get("api_key_header", "x-api-key")
        return APIKeyAuthenticator(
            stream=self,
            key=header,
            value=self.stream_config["api_key"]
        )

    def get_new_paginator(self):
        """Get a paginator for this stream, if configured."""
        pagination_config = self.stream_config.get("pagination")
        if not pagination_config:
            return None

        if pagination_config.get("strategy") == "total_pages":
            return TotalPagesPaginator(
                start_value=1,
                total_pages_path=pagination_config["total_pages_path"],
            )
        return None

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Get URL query parameters."""
        params: Dict[str, Any] = {}
        pagination_config = self.stream_config.get("pagination")

        # Handle pagination params if configured and a token is available
        if pagination_config and next_page_token:
            params[pagination_config["page_param"]] = next_page_token
            if "page_size" in pagination_config:
                params[pagination_config["page_size_param"]] = pagination_config["page_size"]
        return params

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        """Prepare the JSON-encoded request body for the POST request."""
        body = self.stream_config.get("body")
        if not body:
            return None

        # Dynamically inject global start/end dates if placeholders exist
        if "start_date" in body and "start_date" in self.tap.config:
            body["start_date"] = self.tap.config["start_date"]
        if "end_date" in body and "current_date" in self.tap.config:
            body["end_date"] = self.tap.config["current_date"]

        return body

    def parse_response(self, response) -> Iterable[dict]:
        """Parse the response and yield each record."""
        yield from self.extract_jsonpath(self.stream_config["records_path"], response.json())

    # The schema is defined as a property to be read by the parent class.
    @property
    def schema(self) -> dict:
        """Return the JSON schema for this stream."""
        return self.stream_config["schema"]
    