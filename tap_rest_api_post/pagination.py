from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BasePageNumberPaginator

class TotalPagesPaginator(BasePageNumberPaginator):
    """
    A paginator that stops when the current page exceeds the 'totalPages'
    value found in the response body.
    """
    def __init__(self, start_value: int, total_pages_path: str):
        super().__init__(start_value)
        self.total_pages_path = total_pages_path
        self.total_pages = None

    def has_more(self, response) -> bool:
        """Determines if there are more pages to fetch."""
        if self.total_pages is None:
            self.logger.info(f"Attempting to find 'totalPages' with JSONPath: '{self.total_pages_path}'")
            results = list(extract_jsonpath(self.total_pages_path, response.json()))
            if not results:
                self.logger.warning("Could not find 'totalPages' in the response. Assuming only one page.")
                self.total_pages = 1
            else:
                self.total_pages = results[0]
                self.logger.info(f"Found 'totalPages': {self.total_pages}")
        
        has_more_pages = self.current_value <= self.total_pages
        self.logger.info(f"Checking for more pages: Current={self.current_value}, Total={self.total_pages}. Has More? {has_more_pages}")
        
        return has_more_pages
    