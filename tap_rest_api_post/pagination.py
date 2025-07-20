"""Custom pagination classes for the REST API tap."""

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BasePageNumberPaginator


class TotalPagesPaginator(BasePageNumberPaginator):
    """
    A paginator that stops when the current page number meets or exceeds
    the total number of pages discovered in the response body.

    This is designed for APIs like Luganodes that provide a 'totalPages'
    field in their pagination metadata.
    """

    def __init__(self, *args, total_pages_path: str, **kwargs):
        """
        Initialize the paginator.

        Args:
            *args: Paginator positional arguments.
            total_pages_path: JSONPath expression to find the 'totalPages' field.
            **kwargs: Paginator keyword arguments.
        """
        super().__init__(*args, **kwargs)
        self.total_pages_path = total_pages_path

    def has_more(self, response) -> bool:
        """
        Return True if the current page is less than the total number of pages.

        Args:
            response: The HTTP response object.

        Returns:
            True if there are more pages to fetch, otherwise False.
        """
        response_json = response.json()
        # Extract the total number of pages using the provided JSONPath
        total_pages_list = list(extract_jsonpath(self.total_pages_path, response_json))
        if not total_pages_list:
            # If the total pages path is not found, assume only one page
            return False

        total_pages = int(total_pages_list[0])
        current_page = self.current_value

        return current_page < total_pages
