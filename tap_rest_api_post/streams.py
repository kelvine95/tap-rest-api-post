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

    def get_url_params(self, context: Optional[dict], next_page_token: Optional[Any]) -> Dict[str, Any]:
        """
        Get URL query parameters.
        
        For POST requests with pagination, we'll include pagination params in the URL.
        """
        params = {}
        
        # Only add pagination params if we have a paginator and a page token
        if next_page_token is not None:
            cfg = self.stream_config.get("pagination", {})
            page_param = cfg.get("page_param", "page")
            page_size_param = cfg.get("page_size_param", "limit")
            page_size = cfg.get("page_size", 100)
            
            params[page_param] = next_page_token
            params[page_size_param] = page_size
            
            logger.debug(f"[{self.name}] url_params with pagination -> {params}")
        else:
            # First request - set initial pagination params
            cfg = self.stream_config.get("pagination", {})
            if cfg:
                page_param = cfg.get("page_param", "page")
                page_size_param = cfg.get("page_size_param", "limit")
                page_size = cfg.get("page_size", 100)
                start_value = cfg.get("start_value", 1)
                
                params[page_param] = start_value
                params[page_size_param] = page_size
                
                logger.debug(f"[{self.name}] initial url_params -> {params}")
        
        return params

    def prepare_request_payload(self, context: Optional[dict], next_page_token: Optional[Any]) -> Optional[dict]:
        """
        Prepare the request payload (body).
        
        Note: We don't include pagination params in the body - they go in URL params.
        """
        raw = copy.deepcopy(self.stream_config.get("body", {}))
        logger.debug(f"[{self.name}] raw body template -> {raw!r}")

        # Handle template variables in the body
        if "start_date" in raw and raw["start_date"] == "${start_date}":
            last_val = self.get_starting_replication_key_value(context)
            start_val = last_val or self.tap.config.get("start_date")
            if isinstance(start_val, (datetime, date)):
                start_str = start_val.strftime("%Y-%m-%d")
            else:
                start_str = str(start_val or "")
            raw["start_date"] = start_str

        if "end_date" in raw and raw["end_date"] == "${current_date}":
            end_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            raw["end_date"] = end_str

        logger.info(f"[{self.name}] payload -> {raw}")
        return raw

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        logger.info(f"[{self.name}] parsing response (status={response.status_code})")
        
        # Log response for debugging
        try:
            response_text = response.text
            logger.debug(f"[{self.name}] response text: {response_text[:500]}...")  # First 500 chars
        except Exception:
            pass
        
        try:
            data = response.json()
        except Exception as e:
            logger.error(f"[{self.name}] invalid JSON response: {e}")
            logger.error(f"[{self.name}] response text: {response.text}")
            raise

        # Check if the API returned success
        if not data.get("success", True):
            logger.error(f"[{self.name}] API returned success=false: {data}")
            return

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

    def validate_response(self, response: requests.Response) -> None:
        """Override to add more detailed error logging."""
        if 400 <= response.status_code < 600:
            msg = f"{response.status_code} {response.reason} for path: {self.path}"
            
            # Try to get more details from response body
            try:
                error_detail = response.json()
                logger.error(f"[{self.name}] API error response: {error_detail}")
            except Exception:
                logger.error(f"[{self.name}] API error response text: {response.text}")
            
            raise Exception(msg)