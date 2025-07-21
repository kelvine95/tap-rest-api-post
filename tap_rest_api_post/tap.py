# tap_rest_api_post/tap.py
import logging
from singer_sdk import Tap
from singer_sdk import typing as th
from tap_rest_api_post.streams import DynamicStream

logger = logging.getLogger(__name__)

class TapRestApiPost(Tap):
    """A generic Meltano tap for REST APIs requiring POST requests, built with detailed logging."""
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
        logger.info(f"Discovering streams: {len(self.config['streams'])}")
        streams = []
        for cfg in self.config['streams']:
            streams.append(DynamicStream(tap=self, config=cfg))
        logger.info(f"Streams discovered: {[s.name for s in streams]}")
        return streams

    def main(self):
        logger.info("Starting tap-rest-api-post...")
        super().main()
        logger.info("tap-rest-api-post finished.")
        