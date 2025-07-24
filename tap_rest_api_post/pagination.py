# tap_rest_api_post/pagination.py
"""Pagination classes for tap-rest-api-post."""

import logging
from typing import Optional, Any

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BasePageNumberPaginator

logger = logging.getLogger(__name__)


class SinglePagePaginator(BasePageNumberPaginator):
    """A paginator for streams that do not have paginated results."""

    def __init__(self):
        """Initialize the paginator with start_value=0."""
        super().__init__(start_value=0)

    def has_more(self, response) -> bool:
        """This paginator always returns False, indicating only one page."""
        return False


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
        logger.debug(f"TotalPagesPaginator initialized with start_value={start_value}, path='{total_pages_path}'")

    def has_more(self, response) -> bool:
        """Check if there are more pages to fetch."""
        if self._total_pages is None:
            # Extract total pages from the first response
            try:
                response_json = response.json()
                logger.debug(f"Looking for total pages at path: {self.total_pages_path}")
                
                all_values = list(extract_jsonpath(self.total_pages_path, response_json))
                if all_values:
                    self._total_pages = int(all_values[0])
                    logger.info(f"Found total pages: {self._total_pages}")
                else:
                    logger.warning(
                        f"Could not find total pages at path '{self.total_pages_path}'. "
                        f"Response keys: {list(response_json.keys())}"
                    )
                    self._total_pages = 1
                    
            except (ValueError, IndexError, KeyError) as e:
                logger.error(f"Error parsing total pages: {e}")
                self._total_pages = 1
            except Exception as e:
                logger.error(f"Unexpected error getting total pages: {e}")
                self._total_pages = 1

        # Check if we have more pages
        has_more = self.current_value < self._total_pages
        logger.debug(f"Page {self.current_value}/{self._total_pages} - has_more: {has_more}")
        
        return has_more

    def get_next(self, response) -> Optional[Any]:
        """Get the next page token."""
        if self.has_more(response):
            return self.current_value + 1
        return None
