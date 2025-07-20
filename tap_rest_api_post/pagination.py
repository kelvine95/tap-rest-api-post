from singer_sdk.helpers.jsonpath import extract_jsonpath

class TotalPagesPaginator:
    """Paginator for APIs that use page-based pagination with total pages count."""
    
    def __init__(self, start_value=1, total_pages_path=None):
        self.current_page = start_value
        self.total_pages_path = total_pages_path
        self.total_pages = None

    def next_page_token(self, response):
        # First response: extract total pages
        if self.total_pages is None:
            matches = extract_jsonpath(self.total_pages_path, response.json())
            if matches:
                self.total_pages = int(matches[0])
            else:
                raise ValueError(f"Total pages not found at path: {self.total_pages_path}")
        
        # Check if more pages exist
        if self.current_page < self.total_pages:
            self.current_page += 1
            return self.current_page
        return None
    