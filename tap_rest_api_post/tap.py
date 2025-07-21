import sys
from singer_sdk import Tap
from singer_sdk import typing as th

from tap_rest_api_post.streams import DynamicStream

class TapRestApiPost(Tap):
    """A generic Meltano tap for REST APIs requiring POST requests."""
    name = "tap-rest-api-post"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "start_date",
            th.StringType,
            description="Default replication start date in YYYY-MM-DD format.",
        ),
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
                    th.Property("params", th.ObjectType()),
                    th.Property("records_path", th.StringType, default="$[*]"),
                    th.Property("primary_keys", th.ArrayType(th.StringType)),
                    th.Property("replication_key", th.StringType),
                    th.Property("record_transform", th.ObjectType()),
                    th.Property(
                        "pagination",
                        th.ObjectType(
                            th.Property("strategy", th.StringType, required=True),
                            th.Property("page_param", th.StringType),
                            th.Property("page_size_param", th.StringType),
                            th.Property("page_size", th.IntegerType),
                            th.Property("total_pages_path", th.StringType),
                            th.Property("start_value", th.IntegerType, default=1),
                        ),
                    ),
                    th.Property(
                        "schema",
                        th.ObjectType(additional_properties=True),
                        required=True,
                    ),
                )
            ),
            required=True,
        ),
    ).to_dict()

    def discover_streams(self):
        """Instantiate one DynamicStream per configured stream."""
        self.logger.info("Starting stream discovery...")
        streams = [
            DynamicStream(tap=self, name=cfg["name"], config=cfg)
            for cfg in self.config.get("streams", [])
        ]
        self.logger.info(f"Discovered {len(streams)} streams.")
        return streams

if __name__ == "__main__":
    TapRestApiPost.cli()