from singer_sdk.authenticators import APIKeyAuthenticator


class HeaderAPIKeyAuthenticator(APIKeyAuthenticator):
    """Authenticator sending the API key in the headers."""

    def __init__(self, stream, key: str, value: str):
        super().__init__(stream=stream, http_headers={key: value})
