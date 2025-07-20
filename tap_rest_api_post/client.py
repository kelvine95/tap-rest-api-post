import copy
import json
from datetime import datetime, timezone
from singer_sdk.streams import RESTStream
from tap_rest_api_post.pagination import TotalPagesPaginator

class PostRESTStream(RESTStream):
    """Base for POST streams with dynamic payloads + pagination."""

    @property
    def http_method(self) -> str:
        return "POST"

    def get_new_paginator(self):
        cfg = self.config.get("pagination", {})
        if cfg.get("strategy") == "total_pages":
            return TotalPagesPaginator(
                start_value=1,
                total_pages_path=cfg.get("total_pages_path")
            )
        return None

    def get_url_params(self, context, next_page_token) -> dict:
        params = dict(self.config.get("params", {}))
        if next_page_token:
            page_param = self.config.get("pagination", {}).get("page_param", "page")
            params[page_param] = next_page_token
        return params

    def prepare_request_payload(self, context, next_page_token) -> dict | None:
        payload = copy.deepcopy(self.config.get("body", {}))
        if not payload:
            return None
            
        # Prepare substitution values
        subs = {
            "start_date": self.config.get("start_date", "2025-01-01"),
            "current_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "current_datetime": datetime.now(timezone.utc).isoformat(),
        }
        
        # Apply substitutions recursively
        return self._recursive_substitute(payload, subs)
    
    def _recursive_substitute(self, obj, subs: dict):
        if isinstance(obj, dict):
            return {k: self._recursive_substitute(v, subs) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._recursive_substitute(elem, subs) for elem in obj]
        if isinstance(obj, str):
            for key, val in subs.items():
                obj = obj.replace(f"${{{key}}}", str(val))
            return obj
        return obj
        