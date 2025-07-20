"""Handles authentication for the tap."""

from singer_sdk.authenticators import APIKeyAuthenticator


class HeaderAPIKeyAuthenticator(APIKeyAuthenticator):
    """Authenticator for API keys passed in the request headers."""

    def __init__(self, stream, key: str, value: str):
        """
        Initializes the authenticator.

        Args:
            stream: The stream instance.
            key: The name of the header key.
            value: The value of the API key.
        """
        super().__init__(stream=stream, http_headers={key: value})
        