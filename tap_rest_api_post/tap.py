# tap.py

import logging
import sys

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_rest_api_post.streams import DynamicStream

# Configure root logger right away for maximum verbosity:
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(name)s:%(lineno)d %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

class TapRestApiPost(Tap):
    """A generic Meltano tap for REST APIs requiring POST requests, built with detailed logging."""
    name = "tap-rest-api-post"

    # root-level config
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

    def discover_streams(self) -> list[DynamicStream]:
        logger.info(f"[Tap] discover_streams → found {len(self.config.get('streams', []))} stream configurations")
        streams = []
        for cfg in self.config.get("streams", []):
            try:
                name = cfg["name"]
                logger.debug(f"[Tap] instantiating DynamicStream for '{name}'")
                streams.append(DynamicStream(tap=self, name=name, config=cfg))
            except KeyError as e:
                logger.error(f"[Tap] missing required stream key: {e}")
                raise
        logger.info(f"[Tap] streams discovered → {[s.name for s in streams]}")
        return streams

    def main(self):
        logger.info("[Tap] starting sync")
        super().main()
        logger.info("[Tap] sync finished")

def main():
    logger.info("[entrypoint] initializing tap-rest-api-post")
    TapRestApiPost.cli()
