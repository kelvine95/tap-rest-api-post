# tap_rest_api_post/auth.py

from singer_sdk.authenticators import APIKeyAuthenticator

class HeaderAPIKeyAuthenticator(APIKeyAuthenticator):
    """Authenticator that sets the API key and Content-Type in the header."""
    def __init__(self, stream, key: str, value: str):
        super().__init__(stream=stream, key=key, value=value, location="header")
        self.logger.info(f"HeaderAPIKeyAuthenticator initialized for stream '{self.stream.name}'.")

    @property
    def auth_headers(self) -> dict:
        headers = super().auth_headers
        headers["Content-Type"] = "application/json"
        self.logger.debug(f"Auth headers prepared: {headers}")
        return headers
