# streams.py

import copy
import logging
from datetime import datetime, timezone, date
from typing import Any, Dict, Iterable, Optional

import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream

from tap_rest_api_post.auth import HeaderAPIKeyAuthenticator
from tap_rest_api_post.pagination import TotalPagesPaginator

logger = logging.getLogger(__name__)

class PostRESTStream(RESTStream):
    """Base class enforcing POST HTTP method with logging."""
    @property
    def http_method(self) -> str:
        logger.debug(f"[{self.name}] http_method -> POST")
        return "POST"

class DynamicStream(PostRESTStream):
    """Dynamic stream supporting configurable POST body, pagination, and extensive logging."""

    def __init__(self, tap, name: str, config: dict):
        self.stream_config = config
        logger.info(f"[DynamicStream __init__] name='{name}', config_keys={list(config.keys())}")
        super().__init__(tap=tap, name=name, schema=self.stream_config["schema"])

    @property
    def url_base(self) -> str:
        base = self.stream_config["api_url"]
        logger.debug(f"[{self.name}] url_base -> {base}")
        return base

    @property
    def path(self) -> str:
        p = self.stream_config["path"]
        logger.debug(f"[{self.name}] path -> {p}")
        return p

    @property
    def authenticator(self) -> HeaderAPIKeyAuthenticator:
        key = self.stream_config.get("api_key_header", "x-api-key")
        logger.debug(f"[{self.name}] creating HeaderAPIKeyAuthenticator with header='{key}'")
        return HeaderAPIKeyAuthenticator(
            stream=self,
            key=key,
            value=self.stream_config.get("api_key", "")
        )

    @property
    def records_jsonpath(self) -> str:
        path = self.stream_config["records_path"]
        logger.debug(f"[{self.name}] records_jsonpath -> {path}")
        return path

    @property
    def replication_key(self) -> Optional[str]:
        key = self.stream_config.get("replication_key")
        logger.debug(f"[{self.name}] replication_key -> {key}")
        return key

    def get_new_paginator(self):
        cfg = self.stream_config.get("pagination", {})
        if cfg.get("strategy") == "total_pages":
            paginator = TotalPagesPaginator(
                start_value=cfg.get("start_value", 1),
                total_pages_path=cfg.get("total_pages_path", "data.pagination.totalPages")
            )
            logger.info(f"[{self.name}] paginator configured -> {cfg}")
            return paginator

        logger.debug(f"[{self.name}] no paginator (strategy != total_pages)")
        return None

    def get_url_params(self, context: Optional[dict], next_page_token) -> Dict[str, Any]:
        cfg = self.stream_config.get("pagination", {})
        params = {
            cfg.get("page_param", "page"): next_page_token,
            cfg.get("page_size_param", "limit"): cfg.get("page_size"),
        }
        logger.debug(f"[{self.name}] url_params -> {params}")
        return params

    def prepare_request_payload(self, context: Optional[dict], next_page_token) -> Optional[dict]:
        raw = copy.deepcopy(self.stream_config.get("body", {}))
        logger.debug(f"[{self.name}] raw body template -> {raw!r}")

        # determine start_date
        last_val = self.get_starting_replication_key_value(context)
        start_val = last_val or self.tap.config.get("start_date")
        if isinstance(start_val, (datetime, date)):
            start_str = start_val.strftime("%Y-%m-%d")
        else:
            start_str = str(start_val or "")
        end_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        raw["start_date"] = start_str
        raw["end_date"] = end_str

        logger.info(f"[{self.name}] payload -> start_date={start_str}, end_date={end_str}")
        logger.debug(f"[{self.name}] prepared payload -> {raw!r}")
        return raw

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        logger.info(f"[{self.name}] parsing response (status={response.status_code})")
        try:
            data = response.json()
        except Exception as e:
            logger.error(f"[{self.name}] invalid JSON response: {e}")
            raise

        records = list(extract_jsonpath(self.records_jsonpath, data))
        logger.info(f"[{self.name}] extracted {len(records)} raw records via JSONPath")

        transform = self.stream_config.get("record_transform", {})
        for rec in records:
            if transform:
                merged = {**rec, **transform}
                logger.debug(f"[{self.name}] transformed record -> {merged!r}")
                yield merged
            else:
                yield rec
