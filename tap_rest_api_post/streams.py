import copy
import requests
from datetime import datetime, timezone, date
from typing import Any, Dict, Iterable, Optional
import logging

from singer_sdk.streams import RESTStream
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_rest_api_post.auth import HeaderAPIKeyAuthenticator
from tap_rest_api_post.pagination import TotalPagesPaginator

logger = logging.getLogger(__name__)

class PostRESTStream(RESTStream):
    """Base class enforcing POST HTTP method."""
    @property
    def http_method(self) -> str:
        return "POST"

class DynamicStream(PostRESTStream):
    """Dynamic stream supporting configurable POST body, pagination, and extensive logging."""

    def __init__(self, tap, name: str, config: dict):
        self.stream_config = config
        super().__init__(tap=tap, name=name, schema=self.stream_config["schema"])
        logger.info(f"DynamicStream initialized for '{name}' with config keys: {list(config.keys())}")

    @property
    def url_base(self) -> str:
        base = self.stream_config["api_url"]
        logger.debug(f"URL base for '{self.name}': {base}")
        return base

    @property
    def path(self) -> str:
        p = self.stream_config["path"]
        logger.debug(f"Path for '{self.name}': {p}")
        return p

    @property
    def authenticator(self) -> HeaderAPIKeyAuthenticator:
        key = self.stream_config.get("api_key_header")
        logger.info(f"Setting auth header '{key}' (value masked)")
        return HeaderAPIKeyAuthenticator(
            stream=self,
            key=key,
            value=self.stream_config.get("api_key")
        )

    @property
    def records_jsonpath(self) -> str:
        path = self.stream_config["records_path"]
        logger.debug(f"Records JSONPath for '{self.name}': {path}")
        return path

    @property
    def replication_key(self) -> Optional[str]:
        key = self.stream_config.get("replication_key")
        logger.debug(f"Replication key for '{self.name}': {key}")
        return key

    def get_new_paginator(self):
        cfg = self.stream_config.get("pagination", {})
        if cfg.get("strategy") == "total_pages":
            paginator = TotalPagesPaginator(
                start_value=cfg.get("start_value", 1),
                total_pages_path=cfg.get("total_pages_path")
            )
            logger.info(f"Paginator configured for '{self.name}': {cfg}")
            return paginator
        logger.debug(f"No paginator needed for '{self.name}'")
        return None

    def get_url_params(self, context: Optional[dict], next_page_token) -> Dict[str, Any]:
        cfg = self.stream_config.get("pagination", {})
        params = {
            cfg["page_param"]: next_page_token,
            cfg["page_size_param"]: cfg.get("page_size"),
        }
        logger.debug(f"URL params for '{self.name}': {params}")
        return params

    def prepare_request_payload(self, context: Optional[dict], next_page_token) -> Optional[dict]:
        raw = copy.deepcopy(self.stream_config.get("body", {}))
        last_val = self.get_starting_replication_key_value(context)
        start_val = last_val or self.config.get("start_date")
        # Convert to YYYY-MM-DD strings
        if isinstance(start_val, (datetime, date)):
            start_str = start_val.strftime("%Y-%m-%d")
        else:
            start_str = str(start_val or "")
        end_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        logger.info(f"Subs for '{self.name}': {{'start_date': '{start_str}', 'current_date': '{end_str}'}}")
        # Directly override to avoid placeholder issues
        payload = raw
        payload["start_date"] = start_str
        payload["end_date"] = end_str
        logger.info(f"Raw payload before subs for '{self.name}': {raw}")
        logger.info(f"Prepared payload for '{self.name}': {payload}")
        return payload

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        logger.info(f"Parsing response for '{self.name}' (status: {response.status_code})")
        data = response.json()
        records = extract_jsonpath(self.records_jsonpath, data)
        logger.debug(f"Extracted {len(records)} records via JSONPath '{self.records_jsonpath}'")
        transform = self.stream_config.get("record_transform", {})
        for rec in records:
            if transform:
                merged = {**rec, **transform}
                logger.debug(f"Transformed record: {merged}")
                yield merged
            else:
                yield rec
                