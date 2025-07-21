import copy
import json
from datetime import datetime, timezone
from singer_sdk.streams import RESTStream
from .pagination import TotalPagesPaginator

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
        
        # --- FIX IS HERE ---
        # Pass the 'context' argument to the get_context_state method.
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
        if isinstance(obj, dict):
            return {k: self._apply_subs(v, subs) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._apply_subs(elem, subs) for elem in obj]
        if isinstance(obj, str):
            for key, value in subs.items():
                obj = obj.replace(f"${{{key}}}", str(value))
            return obj
        return obj
    