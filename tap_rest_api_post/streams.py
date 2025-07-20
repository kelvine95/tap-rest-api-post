"""Defines the dynamic stream class that can be configured from meltano.yml."""

from tap_rest_api_post.client import PostRESTStream
from tap_rest_api_post.auth import HeaderAPIKeyAuthenticator


class DynamicStream(PostRESTStream):
    """
    A highly configurable stream that adapts to different API endpoints.
    Its behavior is defined entirely by the 'config' dictionary passed
    during initialization, which comes from the `meltano.yml` file.
    """

    def __init__(self, tap, name: str, config: dict):
        """
        Initializes the dynamic stream.

        Args:
            tap: The parent tap instance.
            name: The name of the stream.
            config: The configuration dictionary for this specific stream.
        """
        # The schema is passed directly from the config
        super().__init__(tap=tap, name=name, schema=config.get("schema"))
        self.stream_config = config

    @property
    def url_base(self) -> str:
        """Return the API URL root, configured in meltano.yml."""
        return self.stream_config["api_url"]

    @property
    def path(self) -> str:
        """Return the API endpoint path, configured in meltano.yml."""
        return self.stream_config["path"]

    @property
    def primary_keys(self) -> list[str] | None:
        """Return the primary keys, configured in meltano.yml."""
        return self.stream_config.get("primary_keys")

    @property
    def replication_key(self) -> str | None:
        """Return the replication key, configured in meltano.yml."""
        return self.stream_config.get("replication_key")

    @property
    def records_jsonpath(self) -> str:
        """
        Return the JSONPath expression to extract records from the response.
        Defaults to '$[*]' if not provided in meltano.yml.
        """
        return self.stream_config.get("records_path", "$[*]")

    @property
    def authenticator(self) -> HeaderAPIKeyAuthenticator:
        """
        Creates an authenticator instance for the stream.

        It reads the API key and the header name from the stream's
        configuration.
        """
        return HeaderAPIKeyAuthenticator(
            stream=self,
            key=self.stream_config.get("api_key_header", "x-api-key"),
            value=self.stream_config["api_key"]
        )
