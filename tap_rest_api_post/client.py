import copy
import json
from datetime import datetime, timezone
from singer_sdk.streams import RESTStream

class PostRESTStream(RESTStream):
    @property
    def http_method(self) -> str:
        return "POST"

    def get_new_paginator(self):
        cfg = self.config.get("pagination", {})
        if cfg.get("strategy") == "total_pages":
            return TotalPagesPaginator(
                total_pages_path=cfg.get("total_pages_path")
            )
        return None

    def get_url_params(self, context, next_page_token) -> dict:
        params = self.config.get("params", {}).copy()
        if next_page_token:
            params[self.config["pagination"]["page_param"]] = next_page_token
        return params

    def prepare_request_payload(self, context, next_page_token) -> dict:
        payload = copy.deepcopy(self.config.get("body", {}))
        subs = {
            "start_date": self.config.get("start_date"),
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
                obj = obj.replace(f"${{{key}}}", value)
            return obj
        return obj
    