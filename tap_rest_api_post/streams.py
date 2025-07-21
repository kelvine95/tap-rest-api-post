import copy
import logging
from datetime import datetime, timezone, date
import requests
from typing import Iterable, Optional, Any, Dict
from singer_sdk.streams import RESTStream
from singer_sdk.helpers.jsonpath import extract_jsonpath
from tap_rest_api_post.auth import HeaderAPIKeyAuthenticator
from tap_rest_api_post.pagination import TotalPagesPaginator

logger = logging.getLogger(__name__)

class PostRESTStream(RESTStream):
    """Base class forcing POST for all requests, with logging."""
    @property
    def http_method(self) -> str:
        logger.debug(f"HTTP method for stream '{self.name}': POST")
        return "POST"

class DynamicStream(PostRESTStream):
    """Dynamic REST stream for POST-based APIs with incremental support, exact URL assembly, and verbose logging."""
    def __init__(self, tap, config: dict):
        name = config.get("name")
        super().__init__(tap=tap, name=name, schema=config.get("schema"))
        self.stream_config = config
        logger.info(f"Initialized DynamicStream for '{name}' with config keys: {list(config.keys())}")

    @property
    def url_base(self) -> str:
        base = self.stream_config["api_url"].rstrip("/")
        logger.debug(f"url_base for '{self.name}': {base}")
        return base

    @property
    def path(self) -> str:
        raw = self.stream_config.get("path", "")
        p = raw if raw.startswith("/") else f"/{raw}"
        logger.debug(f"path for '{self.name}': {p}")
        return p

    def _full_url(self) -> str:
        url = f"{self.url_base}{self.path}"
        logger.debug(f"Full request URL for '{self.name}': {url}")
        return url

    @property
    def authenticator(self) -> HeaderAPIKeyAuthenticator:
        auth = HeaderAPIKeyAuthenticator(
            stream=self,
            key=self.stream_config.get("api_key_header", "x-api-key"),
            value=self.stream_config.get("api_key"),
        )
        return auth

    @property
    def records_jsonpath(self) -> str:
        path = self.stream_config.get("records_path")
        logger.debug(f"records_jsonpath for '{self.name}': {path}")
        return path

    @property
    def replication_key(self) -> Optional[str]:
        rk = self.stream_config.get("replication_key")
        logger.debug(f"replication_key for '{self.name}': {rk}")
        return rk

    def get_new_paginator(self):
        p_conf = self.stream_config.get("pagination", {})
        if p_conf.get("strategy") == "total_pages":
            paginator = TotalPagesPaginator(
                start_value=p_conf.get("start_value", 1),
                total_pages_path=p_conf.get("total_pages_path", "data.pagination.totalPages"),
            )
            logger.info(f"Paginator configured for '{self.name}' with {p_conf}")
            return paginator
        logger.debug(f"No paginator for '{self.name}': {p_conf.get('strategy')}")
        return None

    def get_url_params(self, context: Optional[dict], next_page_token) -> Dict[str, Any]:
        p_conf = self.stream_config.get("pagination", {})
        params: Dict[str, Any] = {}
        if next_page_token is not None:
            params[p_conf.get("page_param")] = next_page_token
        if p_conf.get("page_size_param") and p_conf.get("page_size"):
            params[p_conf["page_size_param"]] = p_conf["page_size"]
        logger.debug(f"URL params for '{self.name}': {params}")
        return params

    def prepare_request_payload(self, context: Optional[dict], next_page_token) -> Optional[dict]:
        raw = copy.deepcopy(self.stream_config.get("body", {}))
        last_val = self.get_starting_replication_key_value(context)
        start_val = last_val or self.tap.config.get("start_date")
        if isinstance(start_val, (datetime, date)):
            start_str = start_val.strftime("%Y-%m-%d")
        else:
            start_str = str(start_val)
        current_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        subs = {"start_date": start_str, "current_date": current_str}
        logger.debug(f"Raw payload for '{self.name}': {raw}")
        payload = self._apply_subs(raw, subs)
        logger.info(f"Prepared payload for '{self.name}': {payload}")
        return payload

    def _apply_subs(self, obj: Any, subs: dict) -> Any:
        if isinstance(obj, dict):
            return {k: self._apply_subs(v, subs) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._apply_subs(v, subs) for v in obj]
        if isinstance(obj, str):
            result = obj
            for k, v in subs.items():
                result = result.replace(f"${{{k}}}", v)
            return result
        return obj

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        url = self._full_url()
        logger.debug(f"Requesting records from URL '{url}' with params {self.get_url_params(context, None)}")
        return super().request_records(context)

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        logger.info(f"Response status for '{self.name}': {response.status_code}")
        data = response.json()
        records = list(extract_jsonpath(self.records_jsonpath, data))
        logger.info(f"Extracted {len(records)} records for '{self.name}' using path '{self.records_jsonpath}'")
        transform = self.stream_config.get("record_transform", {})
        if transform:
            yield from ({**r, **transform} for r in records)
        else:
            yield from records