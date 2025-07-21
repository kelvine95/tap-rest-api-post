from setuptools import setup, find_packages

setup(
    name="tap-rest-api-post",
    version="0.2.0",
    description="Generic REST API tap supporting POST requests with advanced pagination",
    packages=find_packages(include=["tap_rest_api_post", "tap_rest_api_post.*"]),
    install_requires=[
        "singer-sdk>=0.24.0",
    ],
    entry_points={
        "console_scripts": [
            "tap-rest-api-post = tap_rest_api_post.tap:main"
        ],
    },
)
