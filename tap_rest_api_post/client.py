import copy
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from singer_sdk.streams import RESTStream

from tap_rest_api_post.pagination import TotalPagesPaginator, NoPaginationPaginator


def _recursive_substitute(obj: Any, subs: Dict[str, Any]) -> Any:
    """Recursively substitute ${var} placeholders in strings inside dicts/lists."""
    if isinstance(obj, dict):
        return {k: _recursive_substitute(v, subs) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_recursive_substitute(v, subs) for v in obj]
    if isinstance(obj, str):
        for key, val in subs.items():
            obj = obj.replace(f"${{{key}}}", str(val))
        return obj
    return obj


class PostRESTStream(RESTStream):
    """Base for POST streams with dynamic payloads + pagination."""

    @property
    def http_method(self) -> str:
        return "POST"

    def get_new_paginator(self):
        cfg = self.stream_config.get("pagination") or {}
        strategy = cfg.get("strategy")
        if strategy == "total_pages":
            total_pages_path = cfg.get("total_pages_path")
            if not total_pages_path:
                raise ValueError("`total_pages` strategy requires `total_pages_path` in config.")
            start_val = cfg.get("start_value", 1)
            return TotalPagesPaginator(start_value=start_val, total_pages_path=total_pages_path)
        # fallback to no pagination
        return NoPaginationPaginator()

    def get_url_params(self, context: Dict[str, Any], next_page_token: Optional[int]) -> Dict[str, Any]:
        params = dict(self.stream_config.get("params", {}))
        if next_page_token is not None:
            page_param = self.stream_config.get("pagination", {}).get("page_param", "page")
            params[page_param] = next_page_token
        return params

    def prepare_request_payload(self, context: Dict[str, Any], next_page_token: Optional[int]) -> Optional[Dict[str, Any]]:
        template = self.stream_config.get("body")
        if not template:
            return None
        payload = copy.deepcopy(template)
        # build substitution dict
        subs = {
            "start_date": self.get_starting_replication_key_value(context) or self.config.get("start_date"),
            "current_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "current_datetime": datetime.now(timezone.utc).isoformat(),
            "page": next_page_token or "",
        }
        return _recursive_substitute(payload, subs)
