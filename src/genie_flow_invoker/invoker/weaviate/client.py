from typing import Any, Optional

from genie_flow_invoker.utils import get_config_value
from loguru import logger

import weaviate
from weaviate import WeaviateClient


class WeaviateClientFactory:
    """
    A factory to create Weaviate clients. Maintains a singleton but when that singleton
    is not live, will create a new one.

    Configuration is set at initiation of the factory, and then used for the Weaviate client.

    This factory works like a context manager, so can be used as follows:

    ```
    with WeaviateClientFactory() as client:
        client.collections. ...
    ```

    """

    def __init__(self, config: dict[str, Any]):
        """
        Creates a new Weaviate client factory. Configuration should include: `http_host`,
        `http_port`, `http_secure`, `grpc_host`, `grpc_port`, and `grpc_secure`. The values from
        config will be overriden by environment variables, respectively: `WEAVIATE_HTTP_HOST`,
        `WEAVIATE_HTTP_PORT`, `WEAVIATE_HTTP_SECURE`, `WEAVIATE_GRPC_HOST`, `WEAVIATE_GRPC_PORT` and
        `WEAVIATE_GRPC_SECURE`.
        """
        self._client: Optional[WeaviateClient] = None

        self.http_host = get_config_value(
            config,
            "WEAVIATE_HTTP_HOST",
            "http_host",
            "HTTP Host URI",
        )
        self.http_port = get_config_value(
            config,
            "WEAVIATE_HTTP_PORT",
            "http_port",
            "HTTP Port number",
        )
        self.http_secure = get_config_value(
            config,
            "WEAVIATE_HTTP_SECURE",
            "http_secure",
            "HTTP Secure flag",
        )
        self.grpc_host = get_config_value(
            config,
            "WEAVIATE_GRPC_HOST",
            "grpc_host",
            "GRPC Host URI",
        )
        self.grpc_port = get_config_value(
            config,
            "WEAVIATE_GRPC_PORT",
            "grpc_port",
            "GRPC Port number",
        )
        self.grpc_secure = get_config_value(
            config,
            "WEAVIATE_GRPC_SECURE",
            "grpc_secure",
            "GRPC Secure flag",
        )

    def __enter__(self):
        if self._client is None or not self._client.is_live():
            logger.info("No live weaviate client, creating a new one")
            if self._client is not None:
                self._client.close()
            self._client = weaviate.connect_to_custom(
                self.http_host,
                self.http_port,
                self.http_secure,
                self.grpc_host,
                self.grpc_port,
                self.grpc_secure,
            )
        return self._client

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
