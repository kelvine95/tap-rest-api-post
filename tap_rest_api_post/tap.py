"""The main entry point and stream discovery for the tap."""

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_rest_api_post.streams import DynamicStream


class TapRestApiPost(Tap):
    """A generic Meltano tap for REST APIs requiring POST requests."""
    name = "tap-rest-api-post"

    # Define the configuration schema for the tap.
    # This schema is used by Meltano to validate the configuration provided
    # in the `meltano.yml` file. It ensures all necessary fields are present.
    config_jsonschema = th.PropertiesList(
        th.Property(
            "streams",
            th.ArrayType(
                th.ObjectType(
                    th.Property("name", th.StringType, required=True,
                                description="The name of the stream."),
                    th.Property("api_url", th.StringType, required=True,
                                description="The base URL for the API endpoint."),
                    th.Property("path", th.StringType, required=True,
                                description="The path for the API endpoint (e.g., /rewards/daily)."),
                    th.Property("api_key", th.StringType, required=True, secret=True,
                                description="The API key for authentication."),
                    th.Property("api_key_header", th.StringType, default="x-api-key",
                                description="The name of the HTTP header for the API key."),
                    th.Property("body", th.ObjectType(),
                                description="The JSON body to send with the POST request."),
                    th.Property("params", th.ObjectType(),
                                description="URL query parameters to append to the request."),
                    th.Property("records_path", th.StringType, default="$[*]",
                                description="A JSONPath expression to extract records from the API response."),
                    th.Property("primary_keys", th.ArrayType(th.StringType),
                                description="A list of primary key fields for the stream."),
                    th.Property("replication_key", th.StringType,
                                description="The field to use for incremental replication."),
                    th.Property("pagination", th.ObjectType(
                        th.Property("strategy", th.StringType, required=True,
                                    description="The pagination strategy (e.g., 'page_number')."),
                        th.Property("page_param", th.StringType, required=True,
                                    description="The name of the page number parameter (e.g., 'page').")
                    ), description="Pagination settings for the stream."),
                    th.Property("schema", th.ObjectType(additional_properties=True), required=True,
                                description="The JSON schema defining the stream's records.")
                )
            ),
            required=True,
            description="An array of stream configurations."
        )
    ).to_dict()

    def discover_streams(self) -> list[DynamicStream]:
        """
        Discover and return the list of streams based on the configuration.

        This method iterates through the `streams` array in the meltano.yml
        config and initializes a `DynamicStream` for each one. This is what
        makes the tap generic and configurable.

        Returns:
            A list of initialized stream instances.
        """
        return [
            DynamicStream(
                tap=self,
                name=stream_config["name"],
                config=stream_config
            )
            for stream_config in self.config.get("streams", [])
        ]
    