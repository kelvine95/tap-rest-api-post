# tap_rest_api_post/auth.py

from singer_sdk.authenticators import APIAuthenticatorBase

class HeaderAPIKeyAuthenticator(APIAuthenticatorBase):
    """A stable, compatible authenticator for API key in a request header."""

    def __init__(self, stream, key: str, value: str):
        super().__init__(stream=stream)
        self._key = key
        self._value = value
        self.logger.info("Custom HeaderAPIKeyAuthenticator initialized.")

    def get_auth_header(self) -> dict:
        """
        Returns authorization headers, including the API key and Content-Type.
        This is the correct method to override in the base class.
        """
        self.logger.info(f"Setting auth header '{self._key}' and 'Content-Type'.")
        return {
            self._key: self._value,
            "Content-Type": "application/json",
        }