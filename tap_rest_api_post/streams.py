"""All-in-one Stream class for tap-rest-api-post."""

import copy
import requests
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BasePageNumberPaginator

# --- AUTHENTICATOR (Self-Contained) ---
class HeaderAPIKeyAuthenticator(APIKeyAuthenticator):
    """Authenticator that sets the API key and Content-Type in the header."""
    def __init__(self, stream, key: str, value: str):
        super().__init__(stream=stream, key=key, value=value, location="header")
        self.logger.info(f"HeaderAPIKeyAuthenticator initialized for stream '{self.stream.name}'.")

    @property
    def auth_headers(self) -> dict:
        headers = super().auth_headers
        headers["Content-Type"] = "application/json"
        self.logger.debug(f"Auth headers prepared: {headers}")
        return headers

# --- PAGINATOR (Self-Contained) ---
class TotalPagesPaginator(BasePageNumberPaginator):
    """Paginator that stops when the current page exceeds 'totalPages' from the response."""
    def __init__(self, start_value: int, total_pages_path: str):
        super().__init__(start_value)
        self.total_pages_path = total_pages_path
        self._total_pages = None
        self.logger.info(f"TotalPagesPaginator initialized with start value {start_value}.")

    def has_more(self, response) -> bool:
        """Check if there are more pages to fetch."""
        if self._total_pages is None:
            self.logger.info(f"Attempting to find 'totalPages' with JSONPath: '{self.total_pages_path}'")
            try:
                results = list(extract_jsonpath(self.total_pages_path, response.json()))
                if not results:
                    self.logger.warning("Could not find 'totalPages'. Assuming only one page.")
                    self._total_pages = 1
                else:
                    self._total_pages = int(results[0])
                    self.logger.info(f"Found and set total pages: {self._total_pages}")
            except Exception as e:
                self.logger.error(f"Error parsing 'totalPages' from response: {e}. Stopping pagination.")
                self._total_pages = 1
        
        has_more_pages = self.current_value < self._total_pages
        self.logger.info(f"Pagination check: Next Page={self.current_value + 1}, Total Pages={self._total_pages}. Has More? -> {has_more_pages}")
        return has_more_pages

# --- DYNAMIC STREAM (Self-Contained) ---
class DynamicStream(RESTStream):
    """Dynamic stream class for making POST requests."""
    
    def __init__(self, tap, name: str, config: dict):
        self.stream_config = config
        super().__init__(tap=tap, name=name, schema=self.stream_config["schema"])
        self.logger.info(f"Stream '{self.name}' initialized.")

    @property
    def url_base(self) -> str:
        return self.stream_config["api_url"]

    @property
    def path(self) -> str:
        return self.stream_config["path"]

    @property
    def http_method(self) -> str:
        # This is the most critical fix, hardcoded in the final class.
        return "POST"

    @property
    def authenticator(self) -> HeaderAPIKeyAuthenticator:
        # Uses the self-contained authenticator class defined above.
        return HeaderAPIKeyAuthenticator(
            stream=self,
            key=self.stream_config.get("api_key_header"),
            value=self.stream_config.get("api_key")
        )

    @property
    def records_jsonpath(self) -> str:
        return self.stream_config["records_path"]

    @property
    def replication_key(self) -> Optional[str]:
        return self.stream_config.get("replication_key")

    def get_new_paginator(self):
        pagination_config = self.stream_config.get("pagination")
        if not pagination_config or pagination_config.get("strategy") != "total_pages":
            return None
        return TotalPagesPaginator(
            start_value=pagination_config.get("start_value", 1),
            total_pages_path=pagination_config.get("total_pages_path")
        )

    def get_url_params(self, context: Optional[dict], next_page_token) -> Dict[str, Any]:
        params: dict = {}
        pagination_config = self.stream_config.get("pagination", {})
        
        params[pagination_config["page_param"]] = next_page_token
        params[pagination_config["page_size_param"]] = pagination_config["page_size"]
        
        self.logger.info(f"Preparing URL params: {params}")
        return params

    def prepare_request_payload(self, context: Optional[dict], next_page_token) -> Optional[dict]:
        """Prepare the data payload for the POST request."""
        self.logger.info("Preparing request payload (body)...")
        payload = copy.deepcopy(self.stream_config.get("body", {}))
        
        start_date = self.get_starting_replication_key_value(context)

        if start_date:
            self.logger.info(f"Found state bookmark for '{self.replication_key}': '{start_date}'. Using as start_date.")
        else:
            start_date = self.config.get("start_date")
            self.logger.info(f"No state found. Using config start_date: '{start_date}'.")

        subs = {
            "start_date": start_date,
            "current_date": datetime.now(timezone.utc).strftime("%Y-%m-%d")
        }
        
        self.logger.debug(f"Substituting placeholders with: {subs}")
        final_payload = self._apply_subs(payload, subs)
        self.logger.info(f"Final request body: {final_payload}")
        return final_payload

    def _apply_subs(self, obj, subs):
        """Recursively substitutes placeholder values in the payload."""
        if isinstance(obj, dict):
            return {k: self._apply_subs(v, subs) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._apply_subs(elem, subs) for elem in obj]
        if isinstance(obj, str):
            for key, value in subs.items():
                if value:
                    obj = obj.replace(f"${{{key}}}", str(value))
            return obj
        return obj

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and extract the records."""
        self.logger.info(f"Parsing response from {response.request.method} {response.url}")
        records = extract_jsonpath(self.records_jsonpath, input=response.json())
        
        transform = self.stream_config.get("record_transform", {})
        if transform:
            self.logger.info("Applying record transformation.")
            yield from ({**r, **transform} for r in records)
        else:
            yield from records
            