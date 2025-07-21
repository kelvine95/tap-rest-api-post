# tap_rest_api_post/pagination.py
"""Pagination class for tap-rest-api-post."""

from typing import Optional

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BasePageNumberPaginator


class TotalPagesPaginator(BasePageNumberPaginator):
    """
    Paginator that stops when the page number reaches a 'totalPages'
    value found in the response body.
    """
    def __init__(self, start_value: int, total_pages_path: str):
        """Initialize the paginator."""
        super().__init__(start_value=start_value)
        self.total_pages_path = total_pages_path
        self._total_pages: Optional[int] = None

    def has_more(self, response) -> bool:
        """Check if there are more pages to fetch."""
        if self._total_pages is None:
            # Discover the total number of pages from the first response
            try:
                all_vals = list(extract_jsonpath(self.total_pages_path, response.json()))
                if all_vals:
                    self._total_pages = int(all_vals[0])
                else:
                    # If total_pages_path is not found, assume only one page
                    self.logger.warning(
                        f"Could not find '{self.total_pages_path}' in response. "
                        "Assuming only one page."
                    )
                    self._total_pages = 1
            except (ValueError, IndexError) as e:
                self.logger.error(f"Error parsing total pages: {e}. Defaulting to 1.")
                self._total_pages = 1

        # Compare current page number with the total
        return self.current_value < self._total_pages
    