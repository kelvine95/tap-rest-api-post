import copy
from datetime import datetime, timezone

from singer_sdk.streams import RESTStream
from tap_rest_api_post.pagination import TotalPagesPaginator

class NoPaginationPaginator:
    """A one‑shot paginator for endpoints without pagination."""
    def __init__(self):
        self.finished = False

    def next_page_token(self, response):
        # on first call, mark finished; always returns None to stop
        if not self.finished:
            self.finished = True
        return None

def _recursive_substitute(obj, subs: dict):
    if isinstance(obj, dict):
        return {k: _recursive_substitute(v, subs) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_recursive_substitute(elem, subs) for elem in obj]
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
        cfg = self.stream_config.get("pagination")
        if cfg and cfg.get("strategy") == "total_pages":
            total_pages_path = cfg.get("total_pages_path")
            if not total_pages_path:
                raise ValueError(
                    "Pagination strategy 'total_pages' requires 'total_pages_path'."
                )
            return TotalPagesPaginator(start_value=1, total_pages_path=total_pages_path)
        # No pagination: do exactly one request
        return NoPaginationPaginator()

    def get_url_params(self, context, next_page_token) -> dict:
        params = dict(self.stream_config.get("params", {}))
        if next_page_token and self.stream_config.get("pagination"):
            page_param = self.stream_config["pagination"].get("page_param", "page")
            params[page_param] = next_page_token
        return params

    def prepare_request_payload(self, context, next_page_token) -> dict | None:
        template = self.stream_config.get("body")
        if not template:
            return None
        payload = copy.deepcopy(template)
        start_val = (
            self.get_starting_replication_key_value(context)
            or self.config.get("start_date")
        )
        subs = {
            "start_date": start_val,
            "current_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "current_datetime": datetime.now(timezone.utc).isoformat(),
        }
        return _recursive_substitute(payload, subs)
