from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BasePageNumberPaginator


class TotalPagesPaginator(BasePageNumberPaginator):
    """Paginator that reads the total number of pages from the JSON body."""

    def __init__(self, *args, total_pages_path: str, **kwargs):
        super().__init__(*args, **kwargs)
        self.total_pages_path = total_pages_path

    def has_more(self, response) -> bool:
        data = response.json()
        pages = list(extract_jsonpath(self.total_pages_path, data))
        if not pages:
            return False
        total = int(pages[0])
        return self.current_value < total
