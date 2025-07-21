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
    """Base class enforcing POST HTTP method via property override."""

    @property
    def http_method(self) -> str:
        logger.debug(f"[{self.name}] http_method -> POST")
        return "POST"

class DynamicStream(PostRESTStream):
    """Dynamic stream supporting configurable POST body, pagination, and extensive logging."""

    def __init__(self, tap, name: str, config: dict):
        self.stream_config = config
        logger.info(
            f"[DynamicStream __init__] name='{name}', config_keys={list(config.keys())}"
        )
        super().__init__(tap=tap, name=name, schema=self.stream_config['schema'], path=self.stream_config['path'])

    @property
    def url_base(self) -> str:
        base = self.stream_config['api_url']
        logger.debug(f"[{self.name}] url_base -> {base}")
        return base

    @property
    def path(self) -> str:
        p = self.stream_config['path']
        logger.debug(f"[{self.name}] path -> {p}")
        return p

    @property
    def authenticator(self) -> HeaderAPIKeyAuthenticator:
        key = self.stream_config.get('api_key_header', 'x-api-key')
        logger.debug(f"[{self.name}] creating HeaderAPIKeyAuthenticator with header='{key}'")
        return HeaderAPIKeyAuthenticator(
            stream=self,
            key=key,
            value=self.stream_config.get('api_key', '')
        )

    @property
    def records_jsonpath(self) -> str:
        path = self.stream_config['records_path']
        logger.debug(f"[{self.name}] records_jsonpath -> {path}")
        return path

    @property
    def replication_key(self) -> Optional[str]:
        key = self.stream_config.get('replication_key')
        logger.debug(f"[{self.name}] replication_key -> {key}")
        return key

    def get_new_paginator(self):
        cfg = self.stream_config.get('pagination', {})
        if cfg.get('strategy') == 'total_pages':
            paginator = TotalPagesPaginator(
                start_value=cfg.get('start_value', 1),
                total_pages_path=cfg.get('total_pages_path', 'data.pagination.totalPages')
            )
            logger.info(f"[{self.name}] paginator configured -> {cfg}")
            return paginator
        logger.debug(f"[{self.name}] no paginator configured (strategy != 'total_pages')")
        return None

    def get_url_params(self, context: Optional[dict], next_page_token: Optional[Any]) -> Dict[str, Any]:
        cfg = self.stream_config.get('pagination', {})
        page = next_page_token or cfg.get('start_value', 1)
        params = {
            cfg.get('page_param', 'page'): page,
            cfg.get('page_size_param', 'limit'): cfg.get('page_size', 100)
        }
        logger.debug(f"[{self.name}] url_params -> {params}")
        return params

    def prepare_request_payload(self, context: Optional[dict], next_page_token: Optional[Any]) -> dict:
        raw = copy.deepcopy(self.stream_config.get('body', {}))

        # Determine start_date: last replication key or configured default
        last = self.get_starting_replication_key_value(context)
        start_val = last or self.tap.config.get('start_date')
        if isinstance(start_val, (datetime, date)):
            raw['start_date'] = start_val.strftime('%Y-%m-%d')
        else:
            raw['start_date'] = str(start_val)

        # Determine end_date: always today (UTC)
        raw['end_date'] = datetime.now(timezone.utc).strftime('%Y-%m-%d')

        logger.info(f"[{self.name}] request payload -> {raw}")
        return raw

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        logger.info(f"[{self.name}] parsing response (status={response.status_code})")
        try:
            data = response.json()
        except ValueError as e:
            logger.error(f"[{self.name}] invalid JSON response: {e}")
            logger.error(f"[{self.name}] response text: {response.text}")
            raise

        if not data.get('success', True):
            logger.error(f"[{self.name}] API returned success=false: {data}")
            return []

        records = list(extract_jsonpath(self.records_jsonpath, data))
        logger.info(f"[{self.name}] extracted {len(records)} records via JSONPath '{self.records_jsonpath}'")
        for rec in records:
            yield rec

    def validate_response(self, response: requests.Response) -> None:
        if 400 <= response.status_code < 600:
            msg = f"{response.status_code} {response.reason} for path: {self.path}"
            try:
                detail = response.json()
                logger.error(f"[{self.name}] API error detail: {detail}")
            except ValueError:
                logger.error(f"[{self.name}] API error text: {response.text}")
            raise Exception(msg)

