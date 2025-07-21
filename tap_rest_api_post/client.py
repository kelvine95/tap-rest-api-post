import copy
from datetime import datetime, timezone
from singer_sdk.streams import RESTStream
from .pagination import TotalPagesPaginator

class PostRESTStream(RESTStream):
    """
    A custom REST stream for making POST requests.
    """
    @property
    def http_method(self) -> str:
        return "POST"

    def get_new_paginator(self):
        """Initializes the paginator for the stream."""
        cfg = self.stream_config.get("pagination", {})
        if cfg.get("strategy") == "total_pages":
            return TotalPagesPaginator(
                start_value=cfg.get("start_value", 1),
                total_pages_path=cfg.get("total_pages_path")
            )
        return None

    def get_url_params(self, context, next_page_token) -> dict:
        """
        Prepares the URL query parameters, including pagination.
        """
        params = self.stream_config.get("params", {}).copy()
        pagination = self.stream_config.get("pagination", {})

        if next_page_token:
            params[pagination["page_param"]] = next_page_token

        if page_size := pagination.get("page_size"):
            params[pagination["page_size_param"]] = page_size

        return params

    def prepare_request_payload(self, context, next_page_token) -> dict:
        """
        Prepares the JSON body for the POST request.
        """
        payload = copy.deepcopy(self.stream_config.get("body", {}))
        
        # Correctly pass the context to get the stream's state
        state = self.get_context_state(context)
        
        bookmarks = state.get('bookmarks', {}) if state else {}
        stream_bookmark = bookmarks.get(self.name, {})
        last_record = stream_bookmark.get(self.replication_key, self.config.get("start_date"))

        subs = {
            "start_date": last_record,
            "current_date": datetime.now(timezone.utc).strftime("%Y-%m-%d")
        }
        return self._apply_subs(payload, subs)

    def _apply_subs(self, obj, subs):
        """Recursively substitutes placeholder values in the payload."""
        if isinstance(obj, dict):
            return {k: self._apply_subs(v, subs) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._apply_subs(elem, subs) for elem in obj]
        if isinstance(obj, str):
            for key, value in subs.items():
                obj = obj.replace(f"${{{key}}}", str(value))
            return obj
        return obj
    