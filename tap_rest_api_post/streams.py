from tap_rest_api_post.client import PostRESTStream
from tap_rest_api_post.auth import HeaderAPIKeyAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath

class DynamicStream(PostRESTStream):
    def __init__(self, tap, name: str, config: dict):
        # Set schema before calling super() to ensure it's available
        self._schema = config["schema"]
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
        records = extract_jsonpath(self.records_jsonpath, input=response.json())
        transform = self.stream_config.get("record_transform", {})
        return [self._transform_record(r, transform) for r in records] if transform else records
    
    def _transform_record(self, record, transform):
        return {**record, **transform}
    
    @property
    def authenticator(self):
        return HeaderAPIKeyAuthenticator(
            stream=self,
            key=self.stream_config["api_key_header"],
            value=self.stream_config["api_key"]
        )
    
    @property
    def primary_keys(self) -> list[str]:
        return self.stream_config.get("primary_keys", [])
    
    @property
    def replication_key(self) -> str:
        return self.stream_config.get("replication_key", "")
    
    def get_json_schema(self) -> dict:
        return self._schema
    