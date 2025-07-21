from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BasePageNumberPaginator

class TotalPagesPaginator(BasePageNumberPaginator):
    """
    A paginator that stops when the current page exceeds the 'totalPages'
    value found in the response body.
    """
    def __init__(self, start_value: int, total_pages_path: str):
        """
        Initializes the paginator.

        Args:
            start_value: The initial page number.
            total_pages_path: A JSONPath expression to the 'totalPages' field in the API response.
        """
        super().__init__(start_value)
        self.total_pages_path = total_pages_path
        self.total_pages = None

    def has_more(self, response) -> bool:
        """
        Determines if there are more pages to fetch.

        Args:
            response: The HTTP response from the last request.

        Returns:
            True if there are more pages, False otherwise.
        """
        # Extract the total number of pages from the first API response
        if self.total_pages is None:
            results = extract_jsonpath(self.total_pages_path, response.json())
            self.total_pages = results[0] if results else 0

        # Stop if the next page to fetch is greater than the total number of pages
        return self.current_value <= self.total_pages
    