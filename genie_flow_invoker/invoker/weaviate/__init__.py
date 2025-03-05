from abc import ABC
import json
from hashlib import md5

from loguru import logger
from pydantic_core._pydantic_core import ValidationError

from genie_flow_invoker.genie import GenieInvoker
from genie_flow_invoker.doc_proc import SimilaritySearchRequest
from genie_flow_invoker.invoker.weaviate.client import WeaviateClientFactory
from genie_flow_invoker.invoker.weaviate.model import WeaviateSimilaritySearchRequest
from genie_flow_invoker.invoker.weaviate.search import SimilaritySearcher


class AbstractWeaviateSimilaritySearchInvoker(GenieInvoker, ABC):

    def __init__(
            self,
            connection_config: dict[str],
            query_config: dict[str],
    ) -> None:
        self.client_factory: WeaviateClientFactory = WeaviateClientFactory(connection_config)

    @classmethod
    def from_config(cls, config: dict):
        """
        Creates ab abstract Weaviate SimilaritySearchInvoker from configuration. Configuration
        should include a key `connection` which contains all keys for setting up the connection.
        Should also include the key `query` for all (default) query parameters.
        """
        connection_config = config["connection"]
        query_config = config["query"]
        return cls(
            connection_config,
            query_config,
        )



class WeaviateSimilaritySearchInvoker(AbstractWeaviateSimilaritySearchInvoker):

    def __init__(self, connection_config, query_config) -> None:
        """
        A Genie Invoker to retrieve documents from Weaviate, using similarity search.

        This is the basic Weaviate similarity search invoker that reads search parameters` from
        the `meta.yaml` file that is used to create this invoker.
        """
        super().__init__(connection_config, query_config)
        self.searcher = SimilaritySearcher(self.client_factory, query_config)

    def invoke(self, content: str) -> str:
        """
        This invokes the similarity search, based on a received `SimilaritySearchQuery`
        that is parsed from the `content` parameter. If that parsing is not successful,
        the content is used verbatim.

        If this invoker receives a `SimilaritySearchQuery`, all parameters for the search
        are retrieved from it. If not, they are expected to be part of the configuration
        that is stored in the `meta.yaml` file.

        Will return a JSON version of a list of `ChunkDistance` objects.

        :param content: The text that this similarity search needs to be conducted on.
        :returns: A JSON string containing the results of the similarity search
        """
        logger.debug(f"invoking weaviate near text search with '{content}'")
        logger.info(
            "invoking similarity search for content hash {content_hash}",
            content_hash=md5(content.encode("utf-8")).hexdigest()
        )
        results = self.searcher.search(content)
        json.dumps(
            [
                dict(
                    _id=str(o.uuid),
                    distance=o.metadata.distance,
                    **o.properties,
                )
                for o in results
            ]
        )
