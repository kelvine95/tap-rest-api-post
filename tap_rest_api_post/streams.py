# tap-rest-api-post/streams.py

import copy
import requests
from datetime import datetime, timezone

from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import APIAuthenticatorBase
from singer_sdk.helpers.jsonpath import extract_jsonpath
from .pagination import TotalPagesPaginator

class DynamicStream(RESTStream):
    
    # --- COMBINED AND SIMPLIFIED LOGIC ---

    def __init__(self, tap, name: str, config: dict):
        self.stream_config = config
        super().__init__(
            tap=tap,
            name=name,
            schema=self.stream_config["schema"]
        )
        self.logger.info(f"Stream '{self.name}' initialized.")

    @property
    def url_base(self) -> str:
        return self.stream_config["api_url"]

    @property
    def path(self) -> str:
        return self.stream_config["path"]

    # CRITICAL FIX 1: Force the HTTP method directly here.
    @property
    def http_method(self) -> str:
        return "POST"

    @property
    def authenticator(self) -> APIAuthenticatorBase:
        header = self.stream_config["api_key_header"]
        api_key = self.stream_config["api_key"]
        return APIAuthenticatorBase.create_for_stream(
            self,
            auth_method="api_key",
            header_name=header,
            api_key=api_key
        )

    def get_new_paginator(self):
        return TotalPagesPaginator(
            start_value=1,
            total_pages_path=self.stream_config.get("pagination", {}).get("total_pages_path")
        )

    # CRITICAL FIX 2: Manually build the entire request to ensure it's correct.
    def prepare_request(self, context, next_page_token) -> requests.PreparedRequest:
        """Prepare a POST request with payload in body and pagination in URL params."""
        
        # --- 1. Get State and Determine Dates ---
        state = self.get_context_state(context)
        stream_bookmark = state.get("bookmarks", {}).get(self.name, {})
        last_synced_date = stream_bookmark.get(self.replication_key)

        if last_synced_date:
            start_date = last_synced_date
        else:
            start_date = self.config.get("start_date")

        current_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # --- 2. Prepare Request Body ---
        payload = copy.deepcopy(self.stream_config.get("body", {}))
        payload["start_date"] = start_date
        payload["end_date"] = current_date
        
        # --- 3. Prepare URL Params ---
        params = {
            self.stream_config["pagination"]["page_param"]: next_page_token or 1,
            self.stream_config["pagination"]["page_size_param"]: self.stream_config["pagination"]["page_size"]
        }

        # --- 4. Build Request Object ---
        request = self.build_prepared_request(
            method=self.http_method,
            url=self.get_url(context),
            params=params,
            json=payload
        )
        
        # --- 5. Log Everything ---
        self.logger.info("--- HTTP REQUEST DETAILS (MANUAL BUILD) ---")
        self.logger.info(f"URL: {request.url}")
        self.logger.info(f"METHOD: {request.method}")
        self.logger.info(f"HEADERS: {request.headers}")
        self.logger.info(f"BODY: {request.body.decode('utf-8')}")
        self.logger.info("-------------------------------------------")

        return request

    def parse_response(self, response):
        """Parse the response and extract the records."""
        records = list(extract_jsonpath(self.stream_config["records_path"], input=response.json()))
        self.logger.info(f"Found {len(records)} records in response.")
        
        transform = self.stream_config.get("record_transform", {})
        if transform:
            return [{**r, **transform} for r in records]
        
        return records
    