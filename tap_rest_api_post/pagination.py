from singer_sdk.helpers.jsonpath import extract_jsonpath

class TotalPagesPaginator:
    def __init__(self, start_value=1, total_pages_path=None):
        self.next_page = start_value
        self.total_pages_path = total_pages_path
        self.total_pages = None

    def next_page_token(self, response):
        if self.total_pages is None:
            # Extract total pages from first response
            self.total_pages = extract_jsonpath(self.total_pages_path, response.json())[0]
        
        if self.next_page <= self.total_pages:
            page = self.next_page
            self.next_page += 1
            return page
        return None
        