from typing import Any, Dict, Iterable, Optional
from singer_sdk.streams.rest import RESTStream
from singer_sdk.tap_base import Tap

class DynamicStream(RESTStream):
    """
    A dynamic REST stream that reads its configuration (URL, path, pagination, etc.)
    from the provided stream_config dict.
    """
    def __init__(
        self,
        tap: Tap,
        name: str,
        stream_config: Dict[str, Any]
    ):
        self.stream_config = stream_config
        # Initialize base RESTStream with just tap, name, and schema
        super().__init__(tap=tap, name=name, schema=stream_config.get("schema"))
        # Add API key header to session
        api_key_header = stream_config.get("api_key_header")
        api_key = stream_config.get("api_key")
        if api_key_header and api_key:
            self._session.headers.update({
                api_key_header: api_key,
                "Content-Type": "application/json",
            })

    @property
    def url_base(self) -> str:
        """Base URL for API calls."""
        return self.stream_config.get("api_url", "")

    @property
    def path(self) -> str:
        """Path (endpoint) for this stream."""
        return self.stream_config.get("path", "")

    @property
    def http_method(self) -> str:
        """Use POST as the HTTP method."""
        return "POST"

    @property
    def request_body_json(self) -> Dict[str, Any]:
        """Request body payload for POST."""
        return self.stream_config.get("body", {})

    def get_url_params(
        self,
        context: Optional[Dict[str, Any]],
        next_page_token: Optional[int]
    ) -> Dict[str, Any]:
        """
        Build the query parameters for pagination.
        """
        params: Dict[str, Any] = {}
        pagination = self.stream_config.get("pagination", {})
        page_param = pagination.get("page_param")
        size_param = pagination.get("page_size_param")
        page_size = pagination.get("page_size")

        if page_param and size_param:
            if next_page_token is not None:
                params[page_param] = next_page_token
            else:
                params[page_param] = pagination.get("start_value", 1)
            params[size_param] = page_size
        return params

    def parse_response(self, response: Any) -> Iterable[Dict[str, Any]]:
        """
        Extract records from the API response JSON.
        """
        data = response.json()
        # Drill down to the list of records
        records: Any = data
        for part in self.stream_config.get("records_path", "").split("."):
            records = records.get(part, {})
        if not isinstance(records, list):
            return []

        # Apply any record transformations
        for record in records:
            for key, val in self.stream_config.get("record_transform", {}).items():
                record[key] = val
            yield record

    def next_page_token(self, response: Any) -> Optional[int]:
        """
        Determine the next page number, based on totalPages from the response.
        """
        data = response.json()
        pagination_conf = self.stream_config.get("pagination", {})
        total_pages_path = pagination_conf.get("total_pages_path", "")

        # Navigate to totalPages in JSON
        total_pages: Any = data
        for part in total_pages_path.split("."):
            total_pages = total_pages.get(part, {})
        try:
            total_pages = int(total_pages)
        except (TypeError, ValueError):
            return None

        # Figure out current page from the last request
        last_params = response.request.params or {}
        page_param = pagination_conf.get("page_param")
        try:
            current_page = int(last_params.get(page_param, pagination_conf.get("start_value", 1)))
        except (TypeError, ValueError):
            current_page = pagination_conf.get("start_value", 1)

        if current_page < total_pages:
            return current_page + 1
        return None
