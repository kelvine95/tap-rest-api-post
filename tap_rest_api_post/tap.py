# tap.py
import logging
import sys
from typing import List

from singer_sdk import Tap
from singer_sdk import typing as th
from tap_rest_api_post.streams import DynamicStream

# Root logger configuration: verbose to stderr
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(name)s:%(lineno)d %(message)s",
    stream=sys.stderr,
)

logger = logging.getLogger(__name__)

class TapRestApiPost(Tap):
    """A generic Meltano tap for POST-based REST APIs, with exhaustive logging."""

    name = "tap-rest-api-post"

    config_jsonschema = th.PropertiesList(
        th.Property("start_date", th.StringType, description="Fallback start date (YYYY-MM-DD)"),
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
                    th.Property("pagination", th.ObjectType()),
                    th.Property("schema", th.ObjectType(additional_properties=True), required=True),
                )
            ),
            required=True,
        ),
    ).to_dict()

    def discover_streams(self) -> List[DynamicStream]:
        streams_cfg = self.config.get('streams', [])
        logger.debug(f"[Tap] discover_streams(): {len(streams_cfg)} streams configured")
        return [DynamicStream(self, cfg.get('name'), cfg) for cfg in streams_cfg]

    def run(self, *args, **kwargs):
        logger.info("[Tap] Starting tap-rest-api-post...")
        try:
            super().run(*args, **kwargs)
        except Exception:
            logger.exception("[Tap] Unhandled exception in main loop")
            raise
        logger.info("[Tap] tap-rest-api-post finished successfully.")


def main():
    TapRestApiPost.cli()


if __name__ == '__main__':
    main()