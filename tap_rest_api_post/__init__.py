# tap_rest_api_post/__init__.py
"""tap-rest-api-post: A Singer tap for POST-based REST APIs."""

__version__ = "0.3.1"

from tap_rest_api_post.tap import TapRestApiPost

__all__ = ["TapRestApiPost"]