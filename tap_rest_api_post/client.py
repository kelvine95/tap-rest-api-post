# tap_rest_api_post/client.py

import copy
from datetime import datetime, timezone

from singer_sdk.streams import RESTStream
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
    """
    Base class for POST-based REST streams with dynamic payloads and pagination.
    """

    @property
    def http_method(self) -> str:
        return "POST"

    def get_new_paginator(self):
        """
        Only supports the 'total_pages' strategy. If you need 'page_number'
        you can add a simple paginator here.
        """
        pagination_cfg = self.stream_config.get("pagination") or {}
        if pagination_cfg.get("strategy") == "total_pages":
            total_pages_path = pagination_cfg.get("total_pages_path")
            if not total_pages_path:
                raise ValueError(
                    "Pagination strategy 'total_pages' requires 'total_pages_path'."
                )
            return TotalPagesPaginator(
                start_value=1,
                total_pages_path=total_pages_path,
            )
        return None

    def get_url_params(self, context, next_page_token) -> dict:
        params = dict(self.stream_config.get("params", {}))
        if next_page_token and self.stream_config.get("pagination"):
            page_param = self.stream_config["pagination"].get("page_param", "page")
            params[page_param] = next_page_token
        return params

    def prepare_request_payload(self, context: dict | None, next_page_token) -> dict | None:
        """
        Prepare the JSON payload for the POST request, including variable substitution.
        """
        body_template = self.stream_config.get("body")
        if not body_template:
            return None

        payload = copy.deepcopy(body_template)

        # Determine the 'start_date'—either from state (incremental) or global config
        start_val = (
            self.get_starting_replication_key_value(context)
            or self.config.get("start_date")
        )

        substitutions = {
            "start_date": start_val,
            "current_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "current_datetime": datetime.now(timezone.utc).isoformat(),
        }

        return _recursive_substitute(payload, substitutions)
