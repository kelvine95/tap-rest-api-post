# tap_rest_api_post/auth.py
import logging
from typing import Optional
from singer_sdk.authenticators import SimpleAuthenticator

logger = logging.getLogger(__name__)


class HeaderAPIKeyAuthenticator(SimpleAuthenticator):
    """An authenticator for API key in a request header."""

    def __init__(self, stream, key: str, value: str):
        """Initialize the authenticator with the header key and value."""
        # Initialize with empty auth headers, we'll add them in get_headers
        super().__init__(stream=stream, auth_headers={})
        self._key = key
        self._value = value
        logger.info(
            f"HeaderAPIKeyAuthenticator initialized for stream='{stream.name}' with header='{self._key}' (value masked)"
        )

    @property
    def auth_headers(self) -> dict:
        """Return headers to be merged with any existing headers."""
        headers = {
            self._key: self._value,
        }
        logger.debug(f"[{self._stream.name}] auth_headers -> {self._key}: [MASKED]")
        return headers
