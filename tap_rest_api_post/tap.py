# tap.py
import logging
import sys
from typing import List

# Root logger configuration: send all logs to stderr in a very verbose format
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(name)s:%(lineno)d %(message)s",
    stream=sys.stderr,
)

from singer_sdk import Tap
from singer_sdk import typing as th
from tap_rest_api_post.streams import DynamicStream

logger = logging.getLogger(__name__)

class TapRestApiPost(Tap):
    """A generic Meltano tap for POST-based REST APIs, with exhaustive logging."""
    
    name = "tap-rest-api-post"
    
    config_jsonschema = th.PropertiesList(
        th.Property("start_date", th.DateTimeType),
        th.Property(
            "streams",
            th.ArrayType(
                th.ObjectType(
                    th.Property("name", th.StringType, required=True),
                    th.Property("api_url", th.StringType, required=True),
                    th.Property("path", th.StringType, required=True),
                    th.Property("api_key", th.StringType, required=True, secret=True),
                    th.Property("api_key_header", th.StringType, default="x-api-key"),
                    th.Property("body", th.ObjectType(), required=True),
                    th.Property("records_path", th.StringType, required=True),
                    th.Property("primary_keys", th.ArrayType(th.StringType), required=True),
                    th.Property("replication_key", th.StringType),
                    th.Property("record_transform", th.ObjectType()),
                    th.Property("pagination", th.ObjectType()),
                    th.Property("schema", th.ObjectType(additional_properties=True), required=True),
                )
            ),
            required=True,
        ),
    ).to_dict()

    def discover_streams(self) -> List[DynamicStream]:
        """Instantiate dynamic streams based on configuration with verbose logging."""
        streams_cfg = self.config.get('streams', [])
        logger.debug(f"[Tap] discover_streams(): found {len(streams_cfg)} stream configs")
        
        streams = []
        for cfg in streams_cfg:
            name = cfg.get('name')
            logger.debug(f"[Tap] creating DynamicStream for '{name}' with config keys: {list(cfg.keys())}")
            stream = DynamicStream(tap=self, name=name, config=cfg)
            streams.append(stream)
            logger.info(f"[Tap] added stream: {name}")
        
        logger.debug(f"[Tap] discover_streams(): returning {len(streams)} streams: {[s.name for s in streams]}")
        return streams

    def run(self, *args, **kwargs):
        """Override run method for better error handling."""
        logger.info("[Tap] Starting tap-rest-api-post...")
        try:
            super().run(*args, **kwargs)
        except Exception:
            logger.exception("[Tap] Unhandled exception in main loop")
            raise
        logger.info("[Tap] tap-rest-api-post finished successfully.")

def main():
    """Main entry point for the tap."""
    TapRestApiPost.cli()

if __name__ == "__main__":
    main()
    