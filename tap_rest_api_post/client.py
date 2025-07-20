import copy
from datetime import datetime, timezone
from singer_sdk.streams import RESTStream
from singer_sdk.pagination import PageNumberPaginator

from tap_rest_api_post.pagination import TotalPagesPaginator


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
    """Base class for POST-based REST streams with dynamic payloads and pagination."""

    @property
    def http_method(self) -> str:
        return "POST"

    def get_new_paginator(self):
        cfg = self.stream_config.get("pagination") or {}
        strategy = cfg.get("strategy")
        if strategy == "page_number":
            return PageNumberPaginator(start_value=1)
        if strategy == "total_pages":
            path = cfg.get("total_pages_path")
            if not path:
                raise ValueError(
                    "'total_pages' pagination requires 'total_pages_path'"
                )
            return TotalPagesPaginator(start_value=1, total_pages_path=path)
        return None

    def get_url_params(self, context, next_page_token) -> dict:
        params = dict(self.stream_config.get("params", {}))
        pagination = self.stream_config.get("pagination")
        if pagination and next_page_token:
            page_param = pagination.get("page_param", "page")
            params[page_param] = next_page_token
        return params

    def prepare_request_payload(self, context: dict | None, next_page_token) -> dict | None:
        body = self.stream_config.get("body")
        if not body:
            return None
        payload = copy.deepcopy(body)
        # determine substitution values
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
    