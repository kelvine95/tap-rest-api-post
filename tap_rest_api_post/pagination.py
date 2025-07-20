from typing import Optional
from singer_sdk.helpers.jsonpath import extract_jsonpath
import requests


class TotalPagesPaginator:
    """
    Paginator that retrieves pages based on a total pages value in the API response.

    Args:
        start_value: The starting page number (default 1).
        total_pages_path: A JSONPath expression pointing to the total pages count in the response.
    """
    def __init__(self, start_value: int = 1, total_pages_path: str = "$.data.pagination.totalPages"):
        self._next_page = start_value
        self._total_pages_path = total_pages_path
        self._total_pages: Optional[int] = None
        self.finished: bool = False

    def next_page_token(self, response: requests.Response) -> Optional[int]:
        """
        Return the next page number to request, or None if pagination is complete.
        """
        data = response.json()

        # On first call, extract and cache total pages
        if self._total_pages is None:
            values = extract_jsonpath(self._total_pages_path, data)
            try:
                self._total_pages = int(values[0])
            except Exception:
                self._total_pages = 1

        # If still within bounds, return next page
        if self._next_page <= self._total_pages:
            token = self._next_page
            self._next_page += 1
            return token

        # No more pages
        self.finished = True
        return None


class NoPaginationPaginator:
    """A paginator for endpoints that do not support pagination (single request)."""
    def __init__(self):
        self.finished: bool = False

    def next_page_token(self, response: requests.Response) -> Optional[int]:
        # Allow exactly one request, then mark finished
        if not self.finished:
            self.finished = True
            return None
        return None
