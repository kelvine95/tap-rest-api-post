from tap_rest_api_post.client import PostRESTStream
from tap_rest_api_post.auth import HeaderAPIKeyAuthenticator


class DynamicStream(PostRESTStream):
    """Stream whose behavior (URL, body, pagination, and schema) is driven entirely by config."""

    def __init__(self, tap, name: str, config: dict):
        super().__init__(tap=tap, name=name, schema=config.get("schema"))
        self.stream_config = config

    @property
    def url_base(self) -> str:
        return self.stream_config["api_url"]

    @property
    def path(self) -> str:
        return self.stream_config.get("path", "")

    @property
    def authenticator(self):
        return HeaderAPIKeyAuthenticator(
            stream=self,
            key=self.stream_config.get("api_key_header", "x-api-key"),
            value=self.stream_config["api_key"],
        )

    @property
    def records_jsonpath(self) -> str:
        return self.stream_config.get("records_path", "$[*]")

    @property
    def primary_keys(self):
        return self.stream_config.get("primary_keys")

    @property
    def replication_key(self):
        return self.stream_config.get("replication_key")
