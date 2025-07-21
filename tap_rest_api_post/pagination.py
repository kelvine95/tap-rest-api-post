from singer_sdk.helpers.jsonpath import extract_jsonpath

class TotalPagesPaginator:
    def __init__(self, start_value=1, total_pages_path=None):
        self.next_page = start_value
        self.total_pages_path = total_pages_path
        self.total_pages = None
        self._finished = False

    @property
    def finished(self):
        return self._finished

    def next_page_token(self, response):
        if self.total_pages is None:
            # Extract total pages from first response
            results = extract_jsonpath(self.total_pages_path, response.json())
            self.total_pages = results[0] if results else 0
        
        if self.next_page <= self.total_pages:
            page = self.next_page
            self.next_page += 1
            return page
        
        self._finished = True
        return None
    