"""Contains the generic client for making POST requests."""

import copy
from datetime import datetime, timezone

from singer_sdk.streams import RESTStream
from tap_rest_api_post.pagination import TotalPagesPaginator


def _recursive_substitute(obj, subs: dict):
    """
    Recursively substitute placeholder values in a nested object or list.
    This function is inspired by the logic in the user-provided example.
    """
    if isinstance(obj, dict):
        return {k: _recursive_substitute(v, subs) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_recursive_substitute(elem, subs) for elem in obj]
    if isinstance(obj, str):
        for key, val in subs.items():
            # Replace placeholders like `${start_date}`
            obj = obj.replace(f"${{{key}}}", str(val))
        return obj
    return obj


class PostRESTStream(RESTStream):
    """
    A generic REST stream class that handles POST requests, dynamic payloads,
    and advanced pagination.
    """

    @property
    def http_method(self) -> str:
        """Specifies that this stream uses the POST HTTP method."""
        return "POST"

    def get_new_paginator(self):
        """
        Create a paginator instance based on the stream's configuration.
        This now acts as a factory, selecting the correct paginator class.
        """
        pagination_config = self.stream_config.get("pagination")
        if not pagination_config:
            return None

        strategy = pagination_config.get("strategy")
        if strategy == "total_pages":
            total_pages_path = pagination_config.get("total_pages_path")
            if not total_pages_path:
                raise ValueError(
                    "Pagination strategy 'total_pages' requires 'total_pages_path'."
                )
            return TotalPagesPaginator(
                start_value=1, total_pages_path=total_pages_path
            )

        raise ValueError(f"Unsupported pagination strategy '{strategy}'.")

    def get_url_params(self, context, next_page_token) -> dict:
        """Prepare the URL query parameters for the request."""
        params = self.stream_config.get("params", {}).copy()
        pagination_config = self.stream_config.get("pagination")
        if next_page_token and pagination_config:
            params[pagination_config["page_param"]] = next_page_token
        return params

    def prepare_request_payload(
        self, context: dict | None, next_page_token
    ) -> dict | None:
        """
        Prepare the JSON payload for the POST request, including variable substitution.
        This logic is adapted from the `DynamicPostStream` example.
        """
        body_template = self.stream_config.get("body")
        if not body_template:
            return None

        # Perform a deep copy to avoid modifying the original config
        payload = copy.deepcopy(body_template)

        # Get the start_date from the stream's state if available (for incremental runs)
        # or fall back to the tap's global start_date config.
        start_date_val = self.get_starting_replication_key_value(context) or self.config.get("start_date")

        # Prepare values for substitution
        substitutions = {
            "start_date": start_date_val,
            "current_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "current_datetime": datetime.now(timezone.utc).isoformat(),
        }

        return _recursive_substitute(payload, substitutions)
