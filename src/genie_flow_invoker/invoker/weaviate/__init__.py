import json
from abc import ABC, abstractmethod
from hashlib import md5
from typing import Any, Iterable, Sized

from genie_flow_invoker.genie import GenieInvoker
from loguru import logger
from pydantic_core._pydantic_core import ValidationError
from pydantic.json import pydantic_encoder

from .client import WeaviateClientFactory
from .delete import WeaviateDeleter
from .exceptions import (
    CollectionNotFoundException,
    InvalidFilterException,
    NoMultiTenancySupportException,
    TenantNotFoundException,
    WeaviateDeleteException,
)
from .model import (
    WeaviateDeleteByFilenameRequest,
    WeaviateDeleteByFilterRequest,
    WeaviateDeleteChunksRequest,
    WeaviateDeleteErrorResponse,
    WeaviateDeleteMessage,
    WeaviatePersistenceRequest,
    WeaviatePersistenceResponse,
    WeaviateSimilaritySearchRequest,
)
from .persist import WeaviatePersistor
from .search import AbstractSearcher, SimilaritySearcher, VectorSimilaritySearcher, ChunkedDocumentListModel


class AbstractWeaviateInvoker(GenieInvoker, ABC):

    def __init__(self, client_factory: WeaviateClientFactory):
        self.client_factory = client_factory

    @classmethod
    def create_client_factory(cls, config: dict):
        connection_config = config["connection"]
        return WeaviateClientFactory(connection_config)


class ConfiguredWeaviateSimilaritySearchInvoker(AbstractWeaviateInvoker, ABC):

    def __init__(
        self,
        client_factory: WeaviateClientFactory,
        query_config: dict[str, Any],
    ) -> None:
        super().__init__(client_factory)
        """
        A Genie Invoker to retrieve documents from Weaviate, using similarity search.

        This is the basic Weaviate similarity search invoker that reads search parameters` from
        the `meta.yaml` file that is used to create this invoker.
        """
        self.client_factory = client_factory
        self.query_config = query_config
        self.searcher = self.searcher_class(self.client_factory, self.query_config)

    @classmethod
    def from_config(cls, config: dict):
        """
        Creates ab abstract Weaviate SimilaritySearchInvoker from configuration. Configuration
        should include a key `connection` which contains all keys for setting up the connection.
        Should also include the key `query` for all (default) query parameters.
        """
        client_factory = cls.create_client_factory(config)
        query_config = config["query"]
        return cls(client_factory, query_config)

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
        logger.debug("invoking weaviate with '{content}'", content=content)
        logger.info(
            "invoking similarity search for content hash {content_hash}",
            content_hash=md5(content.encode("utf-8")).hexdigest(),
        )
        search_params = self._parse_input(content)
        results = self.searcher.search(**search_params)
        return ChunkedDocumentListModel.dump_json(results).decode("utf-8")


class WeaviateSimilaritySearchInvoker(ConfiguredWeaviateSimilaritySearchInvoker):
    """
    This Invoker conducts the similarity search, based on the literal content provided.
    Will return a JSON version of a list of `ChunkDistance` objects.
    """

    @property
    def searcher_class(self) -> type[AbstractSearcher]:
        return SimilaritySearcher

    def _parse_input(self, content: str) -> dict[str, Any]:
        logger.debug(
            "invoking similarity search for content {content}",
            content=content,
        )
        logger.info(
            "invoking similarity search for content hash {content_hash}",
            content_hash=md5(content.encode("utf-8")).hexdigest(),
        )
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
        if not isinstance(query_vector, list):
            logger.error("invalid type of query vector '{content}'", content=content)
            raise ValueError("expected a JSON encoded list of floats")
        logger.debug(
            "invoking similarity search for vector of {nr_dims} dimensions, "
            "starting with {first_values}",
            nr_dims=len(query_vector),
            first_values=query_vector[0:3],
        )
        logger.info(
            "invoking similarity search for vector of {nr_dims} dimensions",
            nr_dims=len(query_vector),
        )
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
        logger.debug(
            "invoking similarity search using parameters: {json_query_params}",
            json_query_params=query_params.model_dump_json(),
            **query_params.model_dump(),
        )
        logger.info(
            "invoking similarity search using parameters: {params}",
            params=query_params.model_dump().keys(),
        )
        return query_params.model_dump()


class AbstractWeaviatePersistorInvoker(AbstractWeaviateInvoker, ABC):

    def __init__(
        self, client_factory: WeaviateClientFactory, persist_config: dict
    ) -> None:
        super().__init__(client_factory)
        self.persist_config = persist_config
        self.persistor = WeaviatePersistor(self.client_factory, self.persist_config)

    @classmethod
    def from_config(cls, config: dict):
        client_factory = cls.create_client_factory(config)
        persist_config = config["persist"]
        return cls(client_factory, persist_config)


class WeaviateCreateTenantInvoker(AbstractWeaviatePersistorInvoker):
    """
    This Invoker creates a new tenant within a given Collection. If the tenant within
    that collection already exists, the creation is silently ignored. Will
    return a JSON containing the configuration

    Expects a JSON configuration
    object, containing:

    - collection_name: name of the collection

    - tenant_name: optional name of
    """

    def invoke(self, content: str) -> str:
        try:
            params = json.loads(content)
        except json.decoder.JSONDecodeError:
            logger.error("Cannot parse content as params '{content}'", content=content)
            raise ValueError(f"invalid content '{content}'")

        collection = self.persistor.get_collection(params)
        tenant = self.persistor.create_tenant(collection, params.get("tenant_name", None))
        config = tenant.config.get()
        return json.dumps(
            {
                "collection_name": config.name,
                "description": config.description,
                "multi_tenancy": {
                    "enabled": config.multi_tenancy_config.enabled,
                    "auto_tenant_creation": config.multi_tenancy_config.auto_tenant_creation,
                    "auto_tenant_activation": config.multi_tenancy_config.auto_tenant_activation,
                },
                "properties": {
                    prop.name: str(prop.data_type) for prop in config.properties
                },
            }
        )


class WeaviatePersistInvoker(AbstractWeaviatePersistorInvoker):
    """
    This invoker inserts a Chunked Document into a collection with the given name and potentially
    into a tenant with the given name. Expects a JSON dump of a `WeaviateSimilaritySearchRequest`
    and returns a JSON dump of a `WeaviateSimilaritySearchResponse`.
    """

    def invoke(self, content: str) -> str:
        try:
            request = WeaviatePersistenceRequest.model_validate_json(content)
        except ValidationError as e:
            logger.error(
                "Cannot parse content as persistence request '{content}', error: {error}",
                content=content,
                error=str(e),
            )
            raise ValueError("invalid content '{content}'")

        collection_name, tenant_name, nr_inserted, nr_replaced = (
            self.persistor.persist_document(
                document=request.document,
                collection_name=request.collection_name,
                tenant_name=request.tenant_name,
                batch_size=request.batch_size,
            )
        )

        return WeaviatePersistenceResponse(
            collection_name=collection_name,
            tenant_name=tenant_name,
            nr_inserts=nr_inserted,
            nr_replaces=nr_replaced,
        ).model_dump_json()


class AbstractWeaviateDeleteInvoker(AbstractWeaviateInvoker, ABC):

    def __init__(
        self, client_factory: WeaviateClientFactory, delete_config: dict
    ) -> None:
        super().__init__(client_factory)
        self.delete_config = delete_config
        self.deleter = WeaviateDeleter(self.client_factory, delete_config)

    @classmethod
    def from_config(cls, config: dict):
        client_factory = cls.create_client_factory(config)
        delete_config = config["delete"]
        return cls(client_factory, delete_config)


class WeaviateDeleteChunkInvoker(AbstractWeaviateDeleteInvoker):

    def invoke(self, content: str) -> str:
        try:
            request = WeaviateDeleteChunksRequest.model_validate_json(content)
        except ValidationError as e:
            logger.error("Failed to parse Weaviate Delete Request")
            raise ValueError("Failed to parse Weaviate Delete Request")

        result = self.deleter.delete_chunks_by_id(
            request.chunk_id,
            request.collection_name,
            request.tenant_name,
        )
        return json.dumps(result)


class WeaviateDeleteByFilenameInvoker(AbstractWeaviateDeleteInvoker):

    def invoke(self, content: str) -> str:
        try:
            request = WeaviateDeleteByFilenameRequest.model_validate_json(content)
        except ValidationError as e:
            logger.error("Failed to parse Weaviate Delete by Filename Request")
            raise ValueError("Failed to parse Weaviate Delete by Filename Request")

        result = self.deleter.delete_chunks_by_filename(
            filename=request.filename,
            collection_name=request.collection_name,
            tenant_name=request.tenant_name,
        )
        return json.dumps(result)


class WeaviateDeleteByFilterInvoker(AbstractWeaviateDeleteInvoker):

    def invoke(self, content: str) -> str:
        try:
            filter_definition = WeaviateDeleteByFilterRequest.model_validate_json(
                content
            )
        except json.decoder.JSONDecodeError:
            logger.error("failed to parse filter definition")
            raise ValueError("failed to parse filter definition")

        try:
            result = self.deleter.delete_by_filter(
                filter_definition.model_dump(),
                collection_name=filter_definition.collection_name,
                tenant_name=filter_definition.tenant_name,
            )
            return json.dumps(result)
        except InvalidFilterException as e:
            return WeaviateDeleteErrorResponse(
                collection_name=e.collection_name,
                tenant_name=e.tenant_name,
                error_code=e.__name__,
                error=str(e),
            ).model_dump_json()


class WeaviateDeleteTenantInvoker(AbstractWeaviateDeleteInvoker):

    def invoke(self, content: str) -> str:
        try:
            request = WeaviateDeleteMessage.model_validate_json(content)
        except ValidationError as e:
            logger.error("Failed to parse Weaviate Delete Request")
            raise ValueError("Failed to parse Weaviate Delete Request")

        try:
            result = self.deleter.delete_tenant(
                request.collection_name, request.tenant_name
            )
        except WeaviateDeleteException as e:
            return WeaviateDeleteErrorResponse(
                collection_name=e.collection_name,
                tenant_name=e.tenant_name,
                error_code=e.__class__.__name__,
                error=str(e),
            ).model_dump_json()

        return WeaviateDeleteMessage(
            collection_name=result["collection_name"],
            tenant_name=result["tenant_name"],
        ).model_dump_json()


class WeaviateDeleteCollectionInvoker(AbstractWeaviateDeleteInvoker):

    def invoke(self, content: str) -> str:
        try:
            request = WeaviateDeleteMessage.model_validate_json(content)
        except ValidationError as e:
            logger.error("Failed to parse Weaviate Delete Request")
            raise ValueError("Failed to parse Weaviate Delete Request")

        try:
            result = self.deleter.delete_collection(request.collection_name)
        except CollectionNotFoundException as e:
            return WeaviateDeleteErrorResponse.from_exception(e).model_dump_json(
                exclude_none=True,
            )

        return WeaviateDeleteMessage(
            collection_name=result["collection_name"]
        ).model_dump_json(exclude_none=True)
