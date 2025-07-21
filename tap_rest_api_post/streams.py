"""All-in-one Stream class for tap-rest-api-post."""
import copy
import json
import requests
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

from singer_sdk.streams import RESTStream
from singer_sdk.helpers.jsonpath import extract_jsonpath


class DynamicStream(RESTStream):
    """
    A dynamic stream that reads its configuration from the tap settings.
    This class manually controls the entire request and pagination loop.
    """
    
    def __init__(self, tap, name: str, config: dict):
        """Initialize the dynamic stream."""
        self.stream_config = config
        super().__init__(tap=tap, name=name, schema=self.stream_config["schema"])
        self.logger.info(f"Stream '{self.name}' initialized.")

    @property
    def http_method(self) -> str:
        # Set here for clarity, but the manual request_records method is what truly matters.
        return "POST"
        
    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        """
        Manually builds and sends POST requests, handling pagination.
        This logic is a direct copy of the working Jupyter Notebook script.
        """
        self.logger.info(f"Starting manual request records loop for stream '{self.name}'.")

        # --- 1. Get State and Determine Dates ---
        start_date = self.get_starting_replication_key_value(context)
        if start_date:
            self.logger.info(f"Found state bookmark for '{self.replication_key}': '{start_date}'. Using as start_date.")
        else:
            start_date = self.config.get("start_date")
            self.logger.info(f"No state found. Using config start_date: '{start_date}'.")

        # --- 2. Prepare Static Request Components ---
        url = f"{self.stream_config['api_url']}{self.stream_config['path']}"
        headers = {
            "Content-Type": "application/json",
            self.stream_config['api_key_header']: self.stream_config['api_key']
        }
        
        base_payload = copy.deepcopy(self.stream_config.get("body", {}))
        base_payload['start_date'] = start_date
        base_payload['end_date'] = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # --- 3. Pagination Loop ---
        current_page = self.stream_config.get("pagination", {}).get("start_value", 1)
        total_pages = 1 # Initialize to 1 to run the loop at least once
        
        while current_page <= total_pages:
            params = {
                self.stream_config["pagination"]["page_param"]: current_page,
                self.stream_config["pagination"]["page_size_param"]: self.stream_config["pagination"]["page_size"]
            }

            self.logger.info(f"--- Preparing Manual Request: Page {current_page}/{total_pages or '?'} ---")
            self.logger.info(f"URL: {url}")
            self.logger.info(f"METHOD: {self.http_method}")
            self.logger.info(f"HEADERS: {headers}")
            self.logger.info(f"PARAMS: {params}")
            self.logger.info(f"BODY: {base_payload}")
            
            try:
                response = requests.post(
                    url,
                    headers=headers,
                    params=params,
                    json=base_payload
                )
                response.raise_for_status()
                response_data = response.json()
            except Exception as e:
                self.logger.critical(f"Fatal error during request: {e}")
                self.logger.critical(f"Response Text: {response.text if 'response' in locals() else 'No response'}")
                raise

            # Update total_pages from the first successful response
            if total_pages == 1:
                total_pages_path = self.stream_config.get("pagination", {}).get("total_pages_path")
                pages_found = list(extract_jsonpath(total_pages_path, response_data))
                if pages_found:
                    total_pages = int(pages_found[0])
                    self.logger.info(f"Total pages discovered: {total_pages}")
                else:
                    self.logger.warning(f"Could not find total pages at path '{total_pages_path}'. Assuming 1 page.")

            # Yield records
            records_path = self.stream_config["records_path"]
            records = list(extract_jsonpath(records_path, response_data))
            self.logger.info(f"Found {len(records)} records on page {current_page}.")
            
            transform = self.stream_config.get("record_transform", {})
            for record in records:
                if transform:
                    yield {**record, **transform}
                else:
                    yield record

            current_page += 1
            