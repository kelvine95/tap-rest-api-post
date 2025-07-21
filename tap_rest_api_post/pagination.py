# tap_rest_api_post/pagination.py

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BasePageNumberPaginator

class TotalPagesPaginator(BasePageNumberPaginator):
    def __init__(self, start_value: int, total_pages_path: str):
        super().__init__(start_value)
        self.total_pages_path = total_pages_path
        self._total_pages = None

    def has_more(self, response) -> bool:
        if self._total_pages is None:
            self.logger.info(f"Paginator: Finding 'totalPages' with JSONPath: '{self.total_pages_path}'")
            try:
                results = list(extract_jsonpath(self.total_pages_path, response.json()))
                if not results:
                    self._total_pages = 1
                else:
                    self._total_pages = int(results[0])
                self.logger.info(f"Paginator: Discovered total pages: {self._total_pages}")
            except Exception as e:
                self.logger.error(f"Paginator: Error parsing 'totalPages': {e}. Assuming 1 page.")
                self._total_pages = 1
        
        has_more_pages = self.current_value < self._total_pages
        self.logger.info(f"Paginator check: Next Page={self.current_value + 1}, Total={self._total_pages}. Has More? -> {has_more_pages}")
        return has_more_pages