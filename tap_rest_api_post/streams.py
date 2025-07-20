from tap_rest_api_post.client import PostRESTStream
from tap_rest_api_post.authy import HeaderAPIKeyAuthenticator


class DynamicStream(PostRESTStream):
    """Stream whose behavior (URL, body, pagination, schema) is driven by config."""

    def __init__(self, tap, name: str, config: dict):
        super().__init__(tap=tap, name=name, schema=config.get("schema"))
        self.stream_config = config

    @property
    def url_base(self) -> str:
        return self.stream_config["api_url"]

    @property
    def path(self) -> str:
        return self.stream_config["path"]

    @property
    def records_jsonpath(self) -> str:
        return self.stream_config.get("records_path", "$[*]")

    @property
    def primary_keys(self) -> list[str] | None:
        return self.stream_config.get("primary_keys")

    @property
    def replication_key(self) -> str | None:
        return self.stream_config.get("replication_key")

    @property
    def authenticator(self) -> HeaderAPIKeyAuthenticator:
        return HeaderAPIKeyAuthenticator(
            stream=self,
            key=self.stream_config.get("api_key_header", "x-api-key"),
            value=self.stream_config["api_key"],
        )
    