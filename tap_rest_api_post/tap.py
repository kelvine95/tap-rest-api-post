# tap_rest_api_post/tap.py
"""TapRestApiPost tap class."""

from typing import List

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_rest_api_post.streams import DynamicStream


class TapRestApiPost(Tap):
    """A generic Meltano tap for POST-based REST APIs."""

    name = "tap-rest-api-post"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "start_date",
            th.DateTimeType,
            description="Global start date to be injected into stream request bodies.",
        ),
        th.Property(
            "current_date",
            th.DateTimeType,
            description="Global end date to be injected into stream request bodies.",
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
                    th.Property("body", th.ObjectType(), default={}),
                    th.Property("records_path", th.StringType, required=True),
                    th.Property("primary_keys", th.ArrayType(th.StringType), default=[]),
                    th.Property("replication_key", th.StringType),
                    th.Property(
                        "date_handling",
                        th.ObjectType(
                            th.Property("type", th.StringType, allowed_values=["epoch", "date_string"]),
                            th.Property("start_field", th.StringType),
                            th.Property("end_field", th.StringType),
                        ),
                        description="Configuration for how dates are handled in the request body"
                    ),
                    th.Property(
                        "transformations",
                        th.ObjectType(
                            th.Property("field_mappings", th.ObjectType(additional_properties=th.StringType)),
                            th.Property(
                                "value_transformations", 
                                th.ObjectType(
                                    additional_properties=th.ObjectType(
                                        th.Property("type", th.StringType),
                                        th.Property("divisor", th.NumberType),
                                    )
                                )
                            ),
                            th.Property(
                                "field_extractions",
                                th.ObjectType(
                                    additional_properties=th.ObjectType(
                                        th.Property("source_field", th.StringType, required=True),
                                        th.Property("type", th.StringType, required=True),
                                        th.Property("filter_type", th.StringType),
                                    )
                                ),
                                description="Extract values from nested structures"
                            ),
                        ),
                        description="Field mappings, value transformations, and field extractions"
                    ),
                    th.Property(
                        "pagination",
                        th.ObjectType(
                            th.Property("strategy", th.StringType, required=True),
                            th.Property("page_param", th.StringType),
                            th.Property("page_size_param", th.StringType),
                            th.Property("page_size", th.IntegerType),
                            th.Property("total_pages_path", th.StringType),
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

    def discover_streams(self) -> List[DynamicStream]:
        """Return a list of discovered streams."""
        return [
            DynamicStream(tap=self, config=stream_config)
            for stream_config in self.config["streams"]
        ]


# CLI Execution
if __name__ == "__main__":
    TapRestApiPost.cli()
