# tap_rest_api_post/streams.py
"""Stream class for tap-rest-api-post."""

import logging
import json
from datetime import datetime
from typing import Any, Dict, Iterable, Optional, List, Tuple

from singer_sdk.streams import RESTStream
from singer_sdk.pagination import BaseAPIPaginator
from singer_sdk.authenticators import SimpleAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath


from tap_rest_api_post.pagination import TotalPagesPaginator, SinglePagePaginator

# Get a logger for this module
logger = logging.getLogger(__name__)


class DynamicStream(RESTStream):
    """
    A dynamic REST stream driven entirely by its configuration.
    """
    
    # Force POST method
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
        if not self._cached_authenticator:
            header_key = self.stream_config.get("api_key_header", "x-api-key")
            api_key = self.stream_config["api_key"]
            
            # Create a SimpleAuthenticator with the API key header
            self._cached_authenticator = SimpleAuthenticator(
                stream=self,
                auth_headers={
                    header_key: api_key
                }
            )
            
            logger.info(f"Created authenticator for stream '{self.name}' with header '{header_key}'")
            
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
        else:
            logger.warning(f"Unknown pagination strategy '{strategy}' for stream '{self.name}'. Using SinglePagePaginator.")
            return SinglePagePaginator()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Get URL query parameters."""
        params: Dict[str, Any] = {}
        pagination_config = self.stream_config.get("pagination")

        if pagination_config and next_page_token:
            # Add page parameter
            if "page_param" in pagination_config:
                params[pagination_config["page_param"]] = next_page_token
            
            # Add page size parameter
            if "page_size" in pagination_config and "page_size_param" in pagination_config:
                params[pagination_config["page_size_param"]] = pagination_config["page_size"]
        
        # For the first request, still add page size if configured
        elif pagination_config and "page_size" in pagination_config and "page_size_param" in pagination_config:
            params[pagination_config["page_size_param"]] = pagination_config["page_size"]
            # Also add page=1 for first request if page_param is configured
            if "page_param" in pagination_config:
                params[pagination_config["page_param"]] = 1
                
        logger.debug(f"URL params for stream '{self.name}': {params}")
        return params

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        """Prepare the JSON-encoded request body for the POST request."""
        body = self.stream_config.get("body", {}).copy()
        
        # Handle date injection
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
        
        # Fallback for backward compatibility
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
        
        # Check for replication key in state
        if self.replication_key and context:
            start_value = self.get_starting_replication_key_value(context)
            if start_value:
                if isinstance(start_value, datetime):
                    start_date = start_value.strftime("%Y-%m-%d")
                else:
                    start_date = str(start_value)
        
        # Fall back to config
        if not start_date:
            start_date = self.stream_config.get("start_date") or self._tap.config.get("start_date")
        
        # Get end date
        end_date = self.stream_config.get("end_date") or self._tap.config.get("current_date")
        
        # Default to today if no end date
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
            
            # Extract records using JSONPath
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
        
        # Apply field mappings
        if "field_mappings" in transformations:
            for old_field, new_field in transformations["field_mappings"].items():
                if old_field in row:
                    row[new_field] = row.pop(old_field)
        
        # Apply value transformations
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

        return row

    @property
    def schema(self) -> dict:
        """Return the JSON schema for this stream."""
        return self.stream_config["schema"]
