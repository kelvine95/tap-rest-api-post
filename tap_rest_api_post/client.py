import copy
import json
from datetime import datetime, timezone
from singer_sdk.streams import RESTStream
from singer_sdk.helpers._typing import resolve

class PostRESTStream(RESTStream):
    @property
    def http_method(self) -> str:
        return "POST"

    def get_new_paginator(self):
        cfg = self.stream_config.get("pagination", {})
        if cfg.get("strategy") == "total_pages":
            return TotalPagesPaginator(
                start_value=cfg.get("start_value", 1),
                total_pages_path=cfg.get("total_pages_path")
            )
        return None

    def get_url_params(self, context, next_page_token) -> dict:
        params = self.stream_config.get("params", {}).copy()
        pagination = self.stream_config.get("pagination", {})
        
        if next_page_token:
            params[pagination["page_param"]] = next_page_token
        
        if page_size := pagination.get("page_size"):
            params[pagination["page_size_param"]] = page_size
            
        return params

    def prepare_request_payload(self, context, next_page_token) -> dict:
        payload = copy.deepcopy(self.stream_config.get("body", {}))
        context = self.get_context(context, next_page_token)
        return resolve(payload, context)
    