"""A dlt pipeline to ingest data from the Magic: The Gathering API."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


@dlt.source
def magic_the_gathering_source():
    """Define dlt resources from Magic: The Gathering API endpoints."""
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.magicthegathering.io/v1/",
            "paginator": {
                "type": "header_link",
                "links_next_key": "next",
            },
        },
        "resource_defaults": {
            "write_disposition": "replace",
        },
        "resources": [
            {
                "name": "cards",
                "primary_key": "id",
                "endpoint": {
                    "path": "cards",
                    "data_selector": "cards",
                    "params": {
                        "pageSize": 100,
                    },
                },
            },
            {
                "name": "sets",
                "primary_key": "code",
                "endpoint": {
                    "path": "sets",
                    "data_selector": "sets",
                },
            },
        ],
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name="magic_the_gathering_pipeline",
    destination="duckdb",
    dataset_name="magic_the_gathering_data",
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(magic_the_gathering_source())
    print(load_info)  # noqa: T201
