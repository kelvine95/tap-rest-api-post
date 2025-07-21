import json
from typing import Any, Dict, Iterable, Optional

from singer_sdk.streams import RESTStream
from singer_sdk.tap_base import Tap


def _extract_path(data: Dict[str, Any], path: str, default=None):
    """Utility: extract nested value by dot-separated path."""
    parts = path.split('.') if path else []
    for part in parts:
        if not isinstance(data, dict) or part not in data:
            return default
        data = data.get(part)
    return data if data is not None else default


class DynamicStream(RESTStream):
    """
    A dynamic REST stream driven entirely by configuration.
    Config keys:
      - api_url: base URL
      - path: endpoint path
      - api_key_header: header key for API auth
      - api_key: API key value
      - body: JSON body payload
      - records_path: dot-path to list of records
      - primary_keys: list of primary key field names
      - replication_key: field name to use for incremental replication
      - pagination: dict with strategy=total_pages, page_param, page_size_param, page_size,
        total_pages_path, start_value
      - record_transform: dict of extra fields to inject
      - schema: JSON schema dict
    """
    def __init__(self, tap: Tap, name: str, config: Dict[str, Any]):
        self.stream_config = config
        # Initialize RESTStream with provided schema
        super().__init__(
            tap=tap,
            name=name,
            schema=self.stream_config.get("schema"),
        )
        # Configure replication and primary keys
        self.primary_keys = self.stream_config.get("primary_keys", [])
        self.replication_method = "INCREMENTAL"
        self.replication_key = self.stream_config.get("replication_key")

    @property
    def url_base(self) -> str:
        return self.stream_config.get("api_url")

    @property
    def path(self) -> str:
        return self.stream_config.get("path")

    @property
    def http_method(self) -> str:
        return "POST"

    @property
    def http_headers(self) -> dict:
        headers = super().http_headers.copy() if super().http_headers else {}
        api_key_header = self.stream_config.get("api_key_header")
        api_key = self.stream_config.get("api_key")
        if api_key_header and api_key:
            headers[api_key_header] = api_key
        return headers

    def get_body(self, context: Optional[dict]) -> Optional[bytes]:
        body = self.stream_config.get("body", {}).copy()
        # support dynamic date range if configured
        if self.tap.config.get("start_date") and "start_date" in body:
            body["start_date"] = self.tap.config.get("start_date")
        if self.tap.config.get("current_date") and "end_date" in body:
            body["end_date"] = self.tap.config.get("current_date")
        return json.dumps(body).encode() if body else None

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> dict:
        """
        Build URL query params for pagination.
        """
        params: Dict[str, Any] = {}
        pagination = self.stream_config.get("pagination") or {}
        if pagination and pagination.get("strategy") == "total_pages":
            page = next_page_token or pagination.get("start_value", 1)
            params[pagination.get("page_param")] = page
            params[pagination.get("page_size_param")] = pagination.get("page_size", 100)
        return params

    def get_next_page_token(
        self, response, previous_token: Optional[int]
    ) -> Optional[int]:
        pagination = self.stream_config.get("pagination") or {}
        if pagination.get("strategy") != "total_pages":
            return None
        total_pages = _extract_path(
            response.json(), pagination.get("total_pages_path", ""), 1
        )
        current = previous_token or pagination.get("start_value", 1)
        if current < total_pages:
            return current + 1
        return None

    def parse_response(self, response) -> Iterable[dict]:
        """
        Extract records from JSON response and apply transformations.
        """
        data = response.json() or {}
        records = _extract_path(data, self.stream_config.get("records_path", ""), [])
        if not isinstance(records, list):
            self.logger.warning(
                f"Expected list at records_path '{self.stream_config.get('records_path')}', got {type(records)}"
            )
            return []
        for rec in records:
            # inject static fields
            rec.update(self.stream_config.get("record_transform", {}))
            yield rec
