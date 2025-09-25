# tap_rest_api_post/streams.py
"""Stream class for tap-rest-api-post."""

import logging
import json
import base64
from datetime import datetime
from typing import Any, Dict, Iterable, Optional, List, Tuple

from singer_sdk.streams import RESTStream
from singer_sdk.pagination import BaseAPIPaginator
from singer_sdk.authenticators import SimpleAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath


from tap_rest_api_post.pagination import TotalPagesPaginator, SinglePagePaginator, StopIfEmptyPaginator

logger = logging.getLogger(__name__)


class DynamicStream(RESTStream):
    """A dynamic REST stream driven entirely by its configuration."""
    
    rest_method = "POST"
    
    def __init__(self, tap, config: Dict[str, Any]):
        """Initialize the dynamic stream."""
        self.stream_config = config
        self._cached_authenticator = None
        super().__init__(tap=tap)

    @property
    def name(self) -> str:
        """Return the stream's name."""
        return self.stream_config["name"]

    @property
    def primary_keys(self) -> Optional[List[str]]:
        """Return the list of primary key fields."""
        return self.stream_config.get("primary_keys", [])

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
    def authenticator(self) -> SimpleAuthenticator:
        """Return a cached authenticator instance."""
        if self._cached_authenticator:
            return self._cached_authenticator

        auth_headers = {}
        if "auth" in self.stream_config:
            auth_config = self.stream_config["auth"]
            strategy = auth_config.get("strategy")
            
            if strategy == "basic":
                username = auth_config.get("username", "")
                password = auth_config.get("password", "")
                user_pass = f"{username}:{password}".encode("utf-8")
                b64_string = base64.b64encode(user_pass).decode("utf-8")
                auth_headers["Authorization"] = f"Basic {b64_string}"
                logger.info(f"Using Basic authentication for stream '{self.name}'.")
            
            elif strategy == "header":
                header_key = auth_config.get("header_key", "x-api-key")
                header_value = auth_config.get("header_value", "")
                auth_headers[header_key] = header_value
                logger.info(f"Using Header authentication for stream '{self.name}'.")
        
        elif "api_key" in self.stream_config:
            header_key = self.stream_config.get("api_key_header", "x-api-key")
            api_key = self.stream_config["api_key"]
            auth_headers[header_key] = api_key
            logger.info(f"Using legacy Header authentication for stream '{self.name}'.")

        self._cached_authenticator = SimpleAuthenticator(stream=self, auth_headers=auth_headers)
        return self._cached_authenticator

    def get_new_paginator(self) -> BaseAPIPaginator:
        """Get a paginator for this stream, if configured."""
        pagination_config = self.stream_config.get("pagination")
        
        if not pagination_config:
            logger.debug(f"No pagination config for stream '{self.name}', using SinglePagePaginator")
            return SinglePagePaginator()

        strategy = pagination_config.get("strategy")
        if strategy == "total_pages":
            logger.debug(f"Using TotalPagesPaginator for stream '{self.name}'")
            return TotalPagesPaginator(
                start_value=1,
                total_pages_path=pagination_config["total_pages_path"],
            )

        elif strategy == "stop_if_empty":
            logger.debug(f"Using StopIfEmptyPaginator for stream '{self.name}'")
            return StopIfEmptyPaginator(
                start_value=1,
                page_size=pagination_config["page_size"],
                records_path=self.stream_config["records_path"],
            )
        else:
            logger.warning(f"Unknown pagination strategy '{strategy}' for stream '{self.name}'. Using SinglePagePaginator.")
            return SinglePagePaginator()

    def get_url_params(self, context: Optional[dict], next_page_token: Optional[Any]) -> Dict[str, Any]:
        """Get URL query parameters."""
        params: Dict[str, Any] = {}
        pagination_config = self.stream_config.get("pagination")

        pagination_in_body = pagination_config and pagination_config.get("pagination_in_body", False)
        
        if pagination_config and not pagination_in_body:
            page_number = next_page_token or 1
            if "page_param" in pagination_config:
                params[pagination_config["page_param"]] = page_number
            if "page_size_param" in pagination_config and "page_size" in pagination_config:
                params[pagination_config["page_size_param"]] = pagination_config["page_size"]
                
        logger.debug(f"URL params for stream '{self.name}': {params}")
        return params

    def prepare_request_payload(self, context: Optional[dict], next_page_token: Optional[Any]) -> Optional[dict]:
        """Prepare the JSON-encoded request body for the POST request."""
        body = self.stream_config.get("body", {}).copy()
        
        pagination_config = self.stream_config.get("pagination")
        if pagination_config and pagination_config.get("pagination_in_body", False):
            page_number = next_page_token or 1
            if "page_param" in pagination_config:
                body[pagination_config["page_param"]] = page_number
            if "page_size_param" in pagination_config and "page_size" in pagination_config:
                 body[pagination_config["page_size_param"]] = pagination_config["page_size"]

        date_handling = self.stream_config.get("date_handling", {})
        if date_handling:
            start_date, end_date = self._get_date_range(context)
            
            if date_handling.get("type") == "epoch":
                if start_date and "start_field" in date_handling:
                    body[date_handling["start_field"]] = self._convert_date_to_epoch(start_date)
                if end_date and "end_field" in date_handling:
                    body[date_handling["end_field"]] = self._convert_date_to_epoch(end_date)
                    
            elif date_handling.get("type") == "date_string":
                if start_date and "start_field" in date_handling:
                    body[date_handling["start_field"]] = start_date
                if end_date and "end_field" in date_handling:
                    body[date_handling["end_field"]] = end_date
        
        else:
            if "start_date" in body and self._tap.config.get("start_date"):
                body["start_date"] = self._tap.config["start_date"]
            if "end_date" in body and self._tap.config.get("current_date"):
                body["end_date"] = self._tap.config["current_date"]

        logger.debug(f"Request payload for stream '{self.name}': {json.dumps(body, indent=2)}")
        return body

    def _get_date_range(self, context: Optional[dict]) -> Tuple[Optional[str], Optional[str]]:
        """Get the date range for the request based on configuration and state."""
        start_date = None
        
        if self.replication_key and context:
            start_value = self.get_starting_replication_key_value(context)
            if start_value:
                if isinstance(start_value, datetime):
                    start_date = start_value.strftime("%Y-%m-%d")
                else:
                    start_date = str(start_value)
        
        if not start_date:
            start_date = self.stream_config.get("start_date") or self._tap.config.get("start_date")
        
        end_date = self.stream_config.get("end_date") or self._tap.config.get("current_date")
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
            
        logger.debug(f"Date range for stream '{self.name}': {start_date} to {end_date}")
        return start_date, end_date

    def _convert_date_to_epoch(self, date_str: str) -> int:
        """Convert a date string to Solana epoch number."""
        date = datetime.strptime(date_str, "%Y-%m-%d")
        epoch_start = datetime(2020, 11, 7)
        days_since_start = (date - epoch_start).days
        epoch = int(days_since_start / 2.5)
        return epoch

    def parse_response(self, response) -> Iterable[dict]:
        """Parse the response and yield each record."""
        try:
            json_response = response.json()
            logger.debug(f"Response structure for stream '{self.name}': {list(json_response.keys())}")
            
            records = list(extract_jsonpath(self.stream_config["records_path"], input=json_response))
            logger.info(f"Extracted {len(records)} records from response for stream '{self.name}'")
            
            yield from records
        except Exception as e:
            logger.error(f"Error parsing response for stream '{self.name}': {e}")
            logger.debug(f"Response content: {response.text}")
            raise

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        """Apply transformations after parsing the response."""
        transformations = self.stream_config.get("transformations", {})
        
        if "field_mappings" in transformations:
            for old_field, new_field in transformations["field_mappings"].items():
                if old_field in row:
                    row[new_field] = row.pop(old_field)
        
        if "value_transformations" in transformations:
            for field, transform_config in transformations["value_transformations"].items():
                if field in row and transform_config.get("type") == "divide":
                    try:
                        divisor = transform_config["divisor"]
                        original_value = row[field]
                        if isinstance(original_value, (int, float, str)):
                            row[field] = float(original_value) / divisor
                        else:
                            logger.warning(f"Cannot divide non-numeric value in field '{field}': {original_value}")
                            row[field] = None
                    except (ValueError, TypeError, ZeroDivisionError) as e:
                        logger.warning(f"Error transforming field '{field}': {e}")
                        row[field] = None
        
        if "field_extractions" in transformations:
            for new_field, extraction_config in transformations["field_extractions"].items():
                source_field = extraction_config.get("source_field")
                extraction_type = extraction_config.get("type")
                
                if source_field in row and extraction_type == "nested_array":
                    array_data = row.get(source_field, [])
                    if isinstance(array_data, list):
                        for item in array_data:
                            if isinstance(item, dict):
                                item_type = item.get("type", "")
                                if item_type == extraction_config.get("filter_type", ""):
                                    if "numeric" in item and "exp" in item:
                                        value = item["numeric"] / (10 ** item["exp"])
                                        row[new_field] = value
                                    elif "text" in item:
                                        row[new_field] = float(item["text"])
                                    break
                elif source_field in row and extraction_type == "first_array_item":
                    array_data = row.get(source_field, [])
                    if isinstance(array_data, list) and len(array_data) > 0:
                        item = array_data[0]
                        if isinstance(item, dict):
                            if "numeric" in item and "exp" in item:
                                value = item["numeric"] / (10 ** item["exp"])
                                row[new_field] = value
                            elif "text" in item:
                                row[new_field] = float(item["text"])

        return row

    @property
    def schema(self) -> dict:
        """Return the JSON schema for this stream."""
        return self.stream_config["schema"]