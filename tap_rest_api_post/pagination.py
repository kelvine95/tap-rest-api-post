# pagination.py

import logging
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BasePageNumberPaginator

logger = logging.getLogger(__name__)

class TotalPagesPaginator(BasePageNumberPaginator):
    """
    Paginator that reads a `totalPages` field from the response JSON
    and increments the page number until it reaches the total, with extensive logging.
    """
    def __init__(self, start_value: int = 1, total_pages_path: str = "data.pagination.totalPages"):
        super().__init__(start_value=start_value)
        self.total_pages_path = total_pages_path
        self._total_pages = None
        logger.info(f"TotalPagesPaginator(init) start_value={start_value}, total_pages_path='{total_pages_path}'")

    def get_next_page_token(self, response):
        logger.debug(f"[Paginator] current_value={self.current_value}")
        if self._total_pages is None:
            logger.info("[Paginator] discovering total pages from response JSON")
            try:
                all_vals = list(extract_jsonpath(self.total_pages_path, response.json()))
                self._total_pages = int(all_vals[0]) if all_vals else self.start_value
                logger.info(f"[Paginator] discovered total_pages={self._total_pages}")
            except Exception as e:
                logger.error(f"[Paginator] error parsing total pages: {e}. defaulting to {self.start_value}")
                self._total_pages = self.start_value

        next_page = self.current_value + 1
        if next_page <= self._total_pages:
            self.current_value = next_page
            logger.debug(f"[Paginator] next_page_token -> {next_page}")
            return next_page

        logger.debug("[Paginator] no more pages to fetch")
        return None

    def has_more(self, response) -> bool:
        more = self.get_next_page_token(response) is not None
        logger.info(f"[Paginator] has_more? current_page={self.current_value} -> {more}")
        return more
