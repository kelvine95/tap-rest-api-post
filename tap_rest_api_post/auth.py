# tap_rest_api_post/auth.py
import logging
from singer_sdk.authenticators import APIAuthenticatorBase

logger = logging.getLogger(__name__)

class HeaderAPIKeyAuthenticator(APIAuthenticatorBase):
    """A stable, compatible authenticator for API key in a request header with detailed logging."""
    def __init__(self, stream, key: str, value: str):
        super().__init__(stream=stream)
        self._key = key
        self._value = value
        logger.info(f"HeaderAPIKeyAuthenticator initialized with key='{self._key}' and value='{self._value[:4]}***'")

    def get_auth_header(self) -> dict:
        """
        Returns authorization headers, including the API key and Content-Type, with debug logs.
        """
        headers = {
            self._key: self._value,
            "Content-Type": "application/json",
        }
        logger.debug(f"Setting auth header: {headers}")
        return headers
    