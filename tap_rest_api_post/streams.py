# tap_rest_api_post/streams.py

import copy
import requests  # <-- THE MISSING IMPORT IS NOW ADDED
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

from singer_sdk.streams import RESTStream
from singer_sdk.helpers.jsonpath import extract_jsonpath
from tap_rest_api_post.auth import HeaderAPIKeyAuthenticator
from tap_rest_api_post.pagination import TotalPagesPaginator

class PostRESTStream(RESTStream):
    """Base class that forces the method to POST."""
    @property
    def http_method(self) -> str:
        return "POST"

class DynamicStream(PostRESTStream):
    """Dynamic stream class that inherits the POST method."""
    
    def __init__(self, tap, name: str, config: dict):
        self.stream_config = config
        super().__init__(tap=tap, name=name, schema=self.stream_config["schema"])

    @property
    def url_base(self) -> str:
        return self.stream_config["api_url"]

    @property
    def path(self) -> str:
        return self.stream_config["path"]

    @property
    def authenticator(self) -> HeaderAPIKeyAuthenticator:
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
        
        return params

    def prepare_request_payload(self, context: Optional[dict], next_page_token) -> Optional[dict]:
        payload = copy.deepcopy(self.stream_config.get("body", {}))
        start_date = self.get_starting_replication_key_value(context) or self.config.get("start_date")

        subs = {
            "start_date": start_date,
            "current_date": datetime.now(timezone.utc).strftime("%Y-%m-%d")
        }
        
        return self._apply_subs(payload, subs)

    def _apply_subs(self, obj, subs):
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
        records = extract_jsonpath(self.records_jsonpath, input=response.json())
        
        transform = self.stream_config.get("record_transform", {})
        if transform:
            yield from ({**r, **transform} for r in records)
        else:
            yield from records
    