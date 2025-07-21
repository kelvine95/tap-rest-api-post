# tap_rest_api_post/tap.py

import logging
import sys

# ───────────────────────────────────────────────────────────────────────────────
# Root logger configuration: send all logs to stderr in a very verbose format
# ───────────────────────────────────────────────────────────────────────────────
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

    def discover_streams(self) -> list[DynamicStream]:
        logger.debug(f"[Tap] discover_streams() called — configured stream count: {len(self.config.get('streams', []))}")
        streams = [
            DynamicStream(tap=self, name=cfg["name"], config=cfg)
            for cfg in self.config.get("streams", [])
        ]
        logger.debug(f"[Tap] discover_streams() completed — instantiated streams: {[s.name for s in streams]}")
        return streams

    def main(self):
        logger.debug("[Tap] main() start")
        super().main()
        logger.debug("[Tap] main() finished")


# Entry point for Meltano
def main():
    TapRestApiPost.cli()
