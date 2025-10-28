from typing import Any, Optional

from genie_flow_invoker.utils import get_config_value
from loguru import logger

import weaviate
from weaviate import WeaviateClient
from weaviate.classes.init import Auth


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
        `http_port`, `http_secure`, `grpc_host`, `grpc_port`, `grpc_secure` and `api_key`
        which is optional.
        
        The values from config will be overriden by environment variables, respectively:
        `WEAVIATE_HTTP_HOST`, `WEAVIATE_HTTP_PORT`, `WEAVIATE_HTTP_SECURE`,
        `WEAVIATE_GRPC_HOST`, `WEAVIATE_GRPC_PORT`, `WEAVIATE_GRPC_SECURE` and
        `WEAVIATE_API_KEY`.
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
        self.api_key = get_config_value(
            config,
            "WEAVIATE_API_KEY",
            "api_key",
            "Weaviate API Key",
            None,
        )

    def __enter__(self):
        if self._client is None or not self._client.is_live():
            logger.info("No live weaviate client, creating a new one")
            if self._client is not None:
                self._client.close()
            
            connection_params = {
                "http_host":     self.http_host,
                "http_port":     self.http_port,
                "http_secure":   self.http_secure,
                "grpc_host":     self.grpc_host,
                "grpc_port":     self.grpc_port,
                "grpc_secure":   self.grpc_secure,
            }

            if self.api_key:
                # If weaviate_api_key is not None or an empty string, add authentication
                connection_params["auth_credentials"] = Auth.api_key(self.api_key)
                logger.info(
                    "Connecting with API Key authentication with key of length {key_length}",
                    key_length=len(self.api_key),
                )

            self._client = weaviate.connect_to_custom(**connection_params)
        return self._client

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
