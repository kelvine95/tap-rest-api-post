import copy
import requests  # <-- Make sure this is imported
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
    
    # --- ADDING THIS METHOD FOR DEBUGGING ---
    def prepare_request(
        self, context: dict | None, next_page_token
    ) -> requests.PreparedRequest:
        """Prepare a request object and log its details for debugging."""
        # This calls the original SDK method to build the request
        request: requests.PreparedRequest = super().prepare_request(
            context, next_page_token
        )

        # --- DEBUGGING LOGS ---
        # These lines will print the request details to your terminal
        self.logger.info("--- HTTP REQUEST DETAILS (FROM TAP) ---")
        self.logger.info(f"URL: {request.url}")
        self.logger.info(f"METHOD: {request.method}")
        self.logger.info(f"HEADERS: {request.headers}")
        if request.body:
            # The body is in bytes, so we decode it for readability
            self.logger.info(f"BODY: {request.body.decode('utf-8')}")
        else:
            self.logger.info("BODY: None")
        self.logger.info("---------------------------------------")
        # --- END DEBUGGING LOGS ---

        return request

    def prepare_request_payload(self, context, next_page_token) -> dict:
        """
        Prepares the JSON body for the POST request.
        """
        payload = copy.deepcopy(self.stream_config.get("body", {}))
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