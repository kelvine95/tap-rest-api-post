import copy
import requests
from datetime import datetime, timezone

from singer_sdk.streams import RESTStream
from .pagination import TotalPagesPaginator

class PostRESTStream(RESTStream):
    """A custom REST stream for making POST requests."""

    # CRITICAL FIX: Explicitly set the HTTP method to POST.
    @property
    def http_method(self) -> str:
        """Explicitly set the HTTP method to POST."""
        self.logger.debug(f"Setting HTTP method to 'POST' for stream '{self.name}'.")
        return "POST"

    def get_new_paginator(self):
        """Initializes the paginator for the stream."""
        cfg = self.stream_config.get("pagination", {})
        if cfg.get("strategy") == "total_pages":
            self.logger.info("TotalPagesPaginator selected.")
            return TotalPagesPaginator(
                start_value=cfg.get("start_value", 1),
                total_pages_path=cfg.get("total_pages_path")
            )
        self.logger.info("No pagination strategy selected.")
        return None

    def get_url_params(self, context, next_page_token) -> dict:
        """Prepares the URL query parameters, including pagination."""
        params = self.stream_config.get("params", {}).copy()
        pagination = self.stream_config.get("pagination", {})

        if next_page_token:
            page_param = pagination["page_param"]
            self.logger.info(f"Adding pagination param: '{page_param}={next_page_token}'")
            params[page_param] = next_page_token

        if page_size := pagination.get("page_size"):
            page_size_param = pagination["page_size_param"]
            self.logger.info(f"Adding page size param: '{page_size_param}={page_size}'")
            params[page_size_param] = page_size

        return params

    def prepare_request(self, context, next_page_token) -> requests.PreparedRequest:
        """Prepare a request object and log its details for debugging."""
        request = super().prepare_request(context, next_page_token)

        self.logger.info("--- HTTP REQUEST DETAILS (FROM TAP) ---")
        self.logger.info(f"URL: {request.url}")
        self.logger.info(f"METHOD: {request.method}")
        self.logger.info(f"HEADERS: {request.headers}")
        if request.body:
            self.logger.info(f"BODY: {request.body.decode('utf-8')}")
        else:
            self.logger.info("BODY: None")
        self.logger.info("---------------------------------------")

        return request

    def prepare_request_payload(self, context, next_page_token) -> dict | None:
        """Prepares the JSON body for the POST request."""
        payload = copy.deepcopy(self.stream_config.get("body", {}))
        
        # CRITICAL FIX: Correctly get the start date from state or config.
        bookmark = self.get_stream_or_partition_state(context)
        last_synced_date = bookmark.get("replication_key_value")

        if last_synced_date:
            start_date = last_synced_date
            self.logger.info(f"Found replication key '{self.replication_key}' in state: '{start_date}'. Using it as start_date.")
        else:
            start_date = self.config.get("start_date")
            self.logger.info(f"No replication key found in state. Falling back to config start_date: '{start_date}'.")

        subs = {
            "start_date": start_date,
            "current_date": datetime.now(timezone.utc).strftime("%Y-%m-%d")
        }
        
        self.logger.info("Substituting date placeholders in the request body...")
        self.logger.debug(f"Placeholder values: {subs}")
        
        return self._apply_subs(payload, subs)

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
    