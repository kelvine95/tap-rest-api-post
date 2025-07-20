from tap_rest_api_post.client import PostRESTStream
from tap_rest_api_post.auth import HeaderAPIKeyAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath

class DynamicStream(PostRESTStream):
    """Stream whose behavior is driven by config."""

    def __init__(self, tap, name: str, config: dict):
        super().__init__(tap=tap, name=name)
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
    
    def parse_response(self, response):
        return extract_jsonpath(self.records_jsonpath, input=response.json())
    
    @property
    def authenticator(self):
        return HeaderAPIKeyAuthenticator(
            stream=self,
            key=self.stream_config["api_key_header"],
            value=self.stream_config["api_key"]
        )
    
    @property
    def primary_keys(self) -> list[str] | None:
        return self.stream_config.get("primary_keys")
    
    @property
    def replication_key(self) -> str | None:
        return self.stream_config.get("replication_key")
    