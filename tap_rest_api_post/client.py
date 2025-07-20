"""Contains the generic client for making POST requests."""

from singer_sdk.streams import RESTStream
from singer_sdk.pagination import PageNumberPaginator


class PostRESTStream(RESTStream):
    """
    A generic REST stream class that handles POST requests and pagination.
    """

    @property
    def http_method(self) -> str:
        """Specifies that this stream uses the POST HTTP method."""
        return "POST"

    def get_new_paginator(self):
        """
        Creates a paginator based on the stream's configuration.

        Returns:
            A PageNumberPaginator if a 'page_number' strategy is defined
            in the stream's config, otherwise None.
        """
        pagination_config = self.stream_config.get("pagination")
        if pagination_config and pagination_config.get("strategy") == "page_number":
            return PageNumberPaginator(start_value=1)
        return None

    def get_url_params(self, context, next_page_token) -> dict:
        """
        Prepare the URL query parameters for the request.

        Args:
            context: The stream context.
            next_page_token: The token for the next page of results.

        Returns:
            A dictionary of URL parameters.
        """
        params = self.stream_config.get("params", {})
        pagination_config = self.stream_config.get("pagination")
        if next_page_token and pagination_config:
            params[pagination_config["page_param"]] = next_page_token
        return params

    def prepare_request_payload(self, context, next_page_token) -> dict | None:
        """
        Prepare the JSON payload for the POST request.

        Args:
            context: The stream context.
            next_page_token: The token for the next page of results.

        Returns:
            A dictionary representing the JSON payload.
        """
        return self.stream_config.get("body")
    