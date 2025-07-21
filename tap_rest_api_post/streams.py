from singer_sdk.helpers.jsonpath import extract_jsonpath
from tap_rest_api_post.auth import HeaderAPIKeyAuthenticator
from tap_rest_api_post.client import PostRESTStream

class DynamicStream(PostRESTStream):
    def __init__(self, tap, name: str, config: dict):
        self._schema = config["schema"]
        super().__init__(tap=tap, name=name)
        self.stream_config = config
        self.logger.info(f"Stream '{self.name}' initialized.")
        self.logger.debug(f"Stream '{self.name}' config: {self.stream_config}")

    @property
    def url_base(self) -> str:
        return self.stream_config["api_url"]

    @property
    def path(self) -> str:
        return self.stream_config["path"]

    @property
    def records_jsonpath(self) -> str:
        # Defaults to `data.rewards` as per your config
        return self.stream_config.get("records_path", "$[*]")

    @property
    def authenticator(self):
        header = self.stream_config["api_key_header"]
        api_key = self.stream_config["api_key"]
        self.logger.info(f"Authenticator created for stream '{self.name}'.")
        self.logger.debug(f"Auth header: '{header}', API Key: '...{api_key[-4:]}'")
        return HeaderAPIKeyAuthenticator(
            stream=self,
            key=header,
            value=api_key
        )
    
    def parse_response(self, response):
        self.logger.info(f"Parsing response for stream '{self.name}'.")
        records = list(extract_jsonpath(self.records_jsonpath, input=response.json()))
        self.logger.info(f"Found {len(records)} records in response.")
        
        transform = self.stream_config.get("record_transform", {})
        if transform:
            self.logger.info("Applying record transformation.")
            return [self._transform_record(r, transform) for r in records]
        
        return records

    def _transform_record(self, record, transform):
        return {**record, **transform}

    @property
    def primary_keys(self) -> list[str]:
        return self.stream_config.get("primary_keys", [])

    @property
    def replication_key(self) -> str:
        return self.stream_config.get("replication_key", "")

    def get_json_schema(self) -> dict:
        return self._schema
    