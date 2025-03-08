from abc import ABC, abstractmethod
import json
from hashlib import md5
from typing import Any

from loguru import logger
from pydantic_core._pydantic_core import ValidationError

from genie_flow_invoker.genie import GenieInvoker
from src.genie_flow_invoker.invoker.weaviate.client import WeaviateClientFactory
from src.genie_flow_invoker.invoker.weaviate.model import WeaviateSimilaritySearchRequest
from src.genie_flow_invoker.invoker.weaviate.search import SimilaritySearcher, \
    AbstractSearcher, VectorSimilaritySearcher


class AbstractWeaviateSimilaritySearchInvoker(GenieInvoker, ABC):

    def __init__(
            self,
            client_factory: WeaviateClientFactory,
            query_config: dict[str, Any],
    ) -> None:
        self.client_factory = client_factory
        self.query_config = query_config

    @classmethod
    def from_config(cls, config: dict):
        """
        Creates ab abstract Weaviate SimilaritySearchInvoker from configuration. Configuration
        should include a key `connection` which contains all keys for setting up the connection.
        Should also include the key `query` for all (default) query parameters.
        """
        connection_config = config["connection"]
        client_factory = WeaviateClientFactory(connection_config)

        query_config = config["query"]
        return cls(client_factory, query_config)


class ConfiguredWeaviateSimilaritySearchInvoker(AbstractWeaviateSimilaritySearchInvoker, ABC):

    def __init__(
            self,
            client_factory: WeaviateClientFactory,
            query_config: dict[str, Any],
    ) -> None:
        """
        A Genie Invoker to retrieve documents from Weaviate, using similarity search.

        This is the basic Weaviate similarity search invoker that reads search parameters` from
        the `meta.yaml` file that is used to create this invoker.
        """
        super().__init__(client_factory, query_config)
        self.searcher = self.searcher_class(self.client_factory, self.query_config)

    @property
    @abstractmethod
    def searcher_class(self) -> type[AbstractSearcher]:
        raise NotImplementedError("Subclasses must implement this method")

    @abstractmethod
    def _parse_input(self, content: str) -> dict[str, Any]:
        raise NotImplementedError()

    def invoke(self, content: str) -> str:
        """
        Execute the similarity search. Content is parsed, based on the searcher that is
        configured for the search invoker class.

        Output is a JSON dump of a list of `ChunkDistance` objects.

        :param content: the content to be processed
        :return: a list of `ChunkDistance` objects
        """
        logger.debug(f"invoking weaviate with '{content}'")
        logger.info(
            "invoking similarity search for content hash {content_hash}",
            content_hash=md5(content.encode("utf-8")).hexdigest()
        )
        search_params = self._parse_input(content)
        results = self.searcher.search(**search_params)
        return json.dumps([result.model_dump() for result in results])


class WeaviateSimilaritySearchInvoker(ConfiguredWeaviateSimilaritySearchInvoker):
    """
    This Invoker conducts the similarity search, based on the literal content provided.
    Will return a JSON version of a list of `ChunkDistance` objects.
    """

    @property
    def searcher_class(self) -> type[AbstractSearcher]:
        return SimilaritySearcher

    def _parse_input(self, content: str) -> dict[str, Any]:
        return dict(query_text=content)


class WeaviateVectorSimilaritySearchInvoker(ConfiguredWeaviateSimilaritySearchInvoker):
    """
    This Invoker conducts a similarity search, given a vector. The vector is expected to be
    passed as a JSON encoded string.
    """

    @property
    def searcher_class(self) -> type[AbstractSearcher]:
        return VectorSimilaritySearcher

    def _parse_input(self, content: str) -> dict[str, Any]:
        try:
            query_vector = json.loads(content)
        except json.decoder.JSONDecodeError:
            logger.error("invalid content '{content}'", content=content)
            raise ValueError("expected a JSON encoded list of floats")
        return dict(query_vector=query_vector)


class WeaviateSimilaritySearchRequestInvoker(ConfiguredWeaviateSimilaritySearchInvoker):
    """
    This Invoker expects a `WeaviateSimilaritySearchRequest` in JSON format.
    """

    @property
    def searcher_class(self) -> type[AbstractSearcher]:
        return VectorSimilaritySearcher

    def _parse_input(self, content: str) -> dict[str, Any]:
        try:
            query_params = WeaviateSimilaritySearchRequest.model_validate_json(content)
        except ValidationError as e:
            logger.error("could not parse invalid content '{content}'", content=content)
            raise ValueError("invalid content '{content}'".format(content=content))
        return query_params.model_dump()

