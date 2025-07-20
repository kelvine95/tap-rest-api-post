"""The main entry point and stream discovery for the tap."""

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_rest_api_post.streams import DynamicStream


class TapRestApiPost(Tap):
    """A generic Meltano tap for REST APIs requiring POST requests."""
    name = "tap-rest-api-post"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The default start date for streams that support replication.",
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
                    th.Property("body", th.ObjectType()),
                    th.Property("params", th.ObjectType()),
                    th.Property("records_path", th.StringType, default="$[*]"),
                    th.Property("primary_keys", th.ArrayType(th.StringType)),
                    th.Property("replication_key", th.StringType),
                    th.Property(
                        "pagination",
                        th.ObjectType(
                            th.Property(
                                "strategy",
                                th.StringType,
                                required=True,
                                description="e.g., 'page_number' or 'total_pages'.",
                            ),
                            th.Property(
                                "page_param",
                                th.StringType,
                                description="The name of the page number parameter (e.g., 'page').",
                            ),
                            th.Property(
                                "total_pages_path",
                                th.StringType,
                                description="JSONPath to the 'totalPages' field in the response.",
                            ),
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

    def discover_streams(self) -> list[DynamicStream]:
        """Discover and return the list of streams based on the configuration."""
        return [
            DynamicStream(tap=self, name=stream_config["name"], config=stream_config)
            for stream_config in self.config.get("streams", [])
        ]
