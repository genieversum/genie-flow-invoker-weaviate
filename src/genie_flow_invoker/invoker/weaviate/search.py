from abc import ABC, abstractmethod
from inspect import signature, Parameter
from typing import Any, Callable, Optional, TypeAlias

from genie_flow_invoker.doc_proc import ChunkedDocument, DocumentChunk
from loguru import logger
from pydantic import TypeAdapter
from weaviate.collections.classes.aggregate import AggregateReturn

from genie_flow_invoker.invoker.weaviate import WeaviateClientFactory
from genie_flow_invoker.invoker.weaviate.base import WeaviateClientProcessor
from genie_flow_invoker.invoker.weaviate.properties import unmap_properties
from genie_flow_invoker.invoker.weaviate.utils import compile_filter
from weaviate.classes.query import Filter, Metrics, QueryReference
from weaviate.collections import Collection
from weaviate.collections.classes.internal import Object


ChunkedDocumentList: TypeAlias = list[ChunkedDocument]
ChunkedDocumentListModel = TypeAdapter(ChunkedDocumentList)


def compile_chunked_documents(
    query_results: list[Object],
    named_vector: str = "default",
) -> list[ChunkedDocument]:
    """
    Given a list of Weaviate Objects, create a list of `ChunkedDocument`s, where each chunked
    document only contains the chunks that were found by the query.

    The Documents and their chunks are maintained in order as follows:
    - from front to back from the query results
    - the first time a document is referenced, a new ChunkedDocument is created
    - every chunk from the same document that comes later is appended to the chunks of their document

    :param query_results: a list of Weaviate Objects that are returned from the query
    :param named_vector: a string representing the named vector or None for the default vector
    :return: a list of ChunkedDocument, com
    """
    if not isinstance(query_results, list):
        logger.error("Query results are not a list, cannot compile chunked documents.")
        raise ValueError(
            "Query results are not a list, cannot compile chunked documents."
        )

    document_index: dict[str, ChunkedDocument] = dict()
    logger.debug(
        "creating a list of chunked document for {nr_chunks} chunks",
        nr_chunks=len(query_results),
    )
    for o in query_results:
        properties: dict[str, Any] = o.properties
        property_map = properties.get("property_map", {})
        unflattened_properties = unmap_properties(properties, property_map)
        chunk = DocumentChunk(
            chunk_id=str(o.uuid),
            content=properties["content"],
            original_span=(
                properties["original_span_start"],
                properties["original_span_end"],
            ),
            hierarchy_level=properties["hierarchy_level"],
            custom_properties=unflattened_properties.get("custom_properties", {}),
            parent_id=str(o.references["parent"].objects[0].uuid) if o.references else None,
            embedding=o.vector[named_vector] if o.vector is not None and len(o.vector) > 0 else None,
        )
        logger.debug(
            "created a chunk with id {chunk_id}",
            **chunk.model_dump(),
        )
        filename = properties["filename"]
        try:
            document_index[filename].chunks.append(chunk)
        except KeyError:
            logger.debug(
                "creating a new ChunkedDocument for filename {filename}",
                filename=filename,
            )
            document_index[filename] = ChunkedDocument(
                filename=filename,
                document_metadata=unflattened_properties.get("document_metadata", {}),
                chunks=[chunk],
            )
    logger.debug(
        "created {nr_documents} chunked documents containing "
        "a total of {nr_chunks} chunks",
        nr_documents=len(document_index.keys()),
        nr_chunks=len(query_results),
    )
    return [document for document in document_index.values()]


def _calculate_operation_level(
    collection: Collection, operation_level: Optional[int]
) -> int:
    logger.debug(
        "calculating operation level {operation_level}",
        operation_level=operation_level,
    )
    response = collection.aggregate.over_all(
        return_metrics=Metrics("hierarchy_level").integer(maximum=True),
    )
    if response is None or not isinstance(response, AggregateReturn):
        logger.error(
            "Failed to retrieve maximum hierarchy level for collection '{collection_name}'",
            collection_name=collection.name,
        )
        raise ValueError(
            f"Failed to retrieve maximum hierarchy level for collection '{collection.name}'"
        )
    logger.debug(
        "found highest hierarchy level {max_hierarchy_level}",
        max_hierarchy_level=response.properties["hierarchy_level"].maximum,
    )
    return response.properties["hierarchy_level"].maximum + operation_level + 1


class AbstractSearcher(WeaviateClientProcessor, ABC):

    def __init__(self, client_factory: WeaviateClientFactory, query_params: dict):
        super().__init__(
            client_factory,
            {
                "collection_name": query_params.get("collection_name", None),
                "tenant_name": query_params.get("tenant_name", None),
            },
        )

        def cast_or_none(dictionary: dict, key: str, data_type: type):
            try:
                return data_type(dictionary[key])
            except (KeyError, TypeError):
                return None

        self.base_query_params = dict(
            parent_strategy=query_params.get("parent_strategy", None),
            operation_level=query_params.get("operation_level", None),
            having_all=query_params.get("having_all", None),
            having_any=query_params.get("having_any", None),
            vector_name=query_params.get("vector_name", None),
            include_vector=bool(query_params.get("include_vector", False)),
            method=query_params.get("method", "cosine"),
            limit=cast_or_none(query_params, "top", int),
            distance=cast_or_none(query_params, "horizon", float),
        )
        logger.debug(
            "setting base query parameters to {base_params}",
            base_params=str(self.base_query_params),
            **self.base_query_params,
        )

    def create_query_params(self, **kwargs) -> dict[str, Any]:
        """
        Creates a dictionary of parameters to pass into the search functions of Weaviate.
        Starting with the base query parameters that are read from the `meta.yaml`, any kwargs
        passed in will be added or override settings from there.

        Special actions are taken as follows:

        - if a named_vector is specified, the target vector is set to it. If not, the target
          vector is set to "default".

        - the standard Genie parameter names are translated to Weaviate parameter names.

        - a filter is compiled based on settings for having_all and having_any.

        - if a parent strategy is specified, the referenced parents are also retrieved

        - a filter is added for any hierarchy level that may be specified

        Only parameters that are usable by the Weaviate query function will be returned, plus
        any kwargs that have been passed into this function.

        :param kwargs: additional keyword arguments to pass to weaviate
        :return: a dictionary of query parameters to be used
        """
        logger.debug(
            "creating query parameters using kwargs {kwargs}",
            kwargs=str(kwargs),
        )
        query_params = self.base_query_params.copy()
        for kwarg_k, kwarg_v in kwargs.items():
            if kwarg_v is not None:
                query_params[kwarg_k] = kwarg_v

        collection = self.get_collection_or_tenant(query_params)
        query_params["collection"] = collection

        translations = {
            "top": "limit",
            "horizon": "distance",
        }
        for genie_param, weaviate_param in translations.items():
            if genie_param in query_params:
                if query_params[genie_param] is not None:
                    query_params[weaviate_param] = query_params[genie_param]
                del query_params[genie_param]

        # if a non-default vector is specified, set the target to it
        query_params["target_vector"] = (
            query_params["vector_name"]
            if query_params.get("vector_name", None) not in [None, ""]
            else "default"
        )

        # if we have a filter, include that into the query parameters
        query_params["filters"] = compile_filter(query_params)

        # if we need the parents, pull in the references too
        if query_params["parent_strategy"] is not None:
            query_params["return_references"] = [QueryReference(link_on="parent")]

        # if we need to operate at a certain level, filter on that level
        if query_params["operation_level"] is not None:
            operation_level = query_params["operation_level"]
            if operation_level < 0:
                operation_level = _calculate_operation_level(collection, operation_level)
            hierarchy_filter = Filter.by_property("hierarchy_level").equal(operation_level)
            if query_params["filters"] is not None:
                query_params["filters"] &= hierarchy_filter
            else:
                query_params["filters"] = hierarchy_filter

        logger.debug(
            "created query parameters for keys: {param_keys}",
            param_keys=query_params.keys(),
        )
        return query_params

    def apply_parent_strategy(
        self,
        query_results: list[Object],
        **kwargs,
    ) -> list[Object]:
        """
        Apply a parent strategy to the query results. The parent strategy is determined from
        the base query configuration, potentially overriden in the kwargs that were passed.

        If the resulting parent strategy is "replace", then only the parents are returned - in
        the same order as their children. Duplicate parents are removed. If the strategy is "include"
        the parents are added to the list of children, deduplicating the parents by having a parent
        follow the child that comes first in order.

        :param query_results: the list of objects returned from the query
        :param kwargs: additional keyword arguments that were passed to the search function
        :return: a list of objects with the parent strategy applied
        """
        if not isinstance(query_results, list):
            logger.error("Query results are not a list, cannot apply parent strategy.")
            raise ValueError("Query results are not a list, cannot apply parent strategy.")
        parent_strategy = kwargs.get(
            "parent_strategy", None
        ) or self.base_query_params.get("parent_strategy", None)
        if parent_strategy is None:
            logger.debug("no parent strategy set")
            return query_results
        logger.debug(
            "parent strategy set to {parent_strategy}", parent_strategy=parent_strategy
        )

        seen_parents = set()
        if parent_strategy == "replace":
            # return a deduplicated list of parents, retaining the order
            parents = list()
            for child in query_results:
                if child.references is None:
                    continue
                for parent in child.references["parent"].objects:
                    if parent.uuid not in seen_parents:
                        parents.append(parent)
                        seen_parents.add(parent.uuid)
            logger.debug(
                "returning {nr_parents} parents from {nr_children} children",
                nr_parents=len(parents),
                nr_children=len(query_results),
            )
            return parents

        # return a combined list of children and their parents, making sure that
        # parents are de-duplicated
        combined = list()
        for child in query_results:
            combined.append(child)
            for parent in child.references["parent"]:
                if parent.uuid not in seen_parents:
                    combined.append(parent)
                    seen_parents.add(parent.uuid)
        logger.debug(
            "returning a combined total of {nr_objects} from {nr_children} children",
            nr_objects=len(combined),
            nr_children=len(query_results),
        )
        return combined

    @abstractmethod
    def _conduct_search(self, collection: Collection) -> Callable:
        """
        Return the function that will conduct the actual search.

        :param collection: the collection to use
        :return: the Callable that will conduct the search
        """
        raise NotImplementedError()

    def search(self, **kwargs) -> list[ChunkedDocument]:
        """
        The function that conducts the actual search. It creates the query parameters, and
        retrieves what Weaviate function to call, then bind the arguments and calls the
        function. The resulting Objects are compiled into a list of ChunkedDocuments.

        :param kwargs: the keyword arguments that will override any configured values
        :return: a list of ChunkedDocuments containing the found chunks
        """
        query_params = self.create_query_params(**kwargs)
        collection = self.get_collection_or_tenant(query_params)
        logger.info(
            "conducting search on collection {collection_name}",
            collection_name=collection.name,
        )

        # bind the necessary arguments to the values in query_params
        search_function = self._conduct_search(collection)
        function_signature = signature(search_function)
        function_params = {
            k:query_params[k] if k in query_params else param.default
            for k, param in function_signature.parameters.items()
        }
        bound_function = function_signature.bind(**function_params)

        # conduct the search and apply the parent strategy
        logger.debug(
            "using search function '{function_name}' with parameters {function_params}",
            function_name=search_function.__name__,
            function_params=str(function_params),
            **function_params,
        )
        query_results = search_function(**bound_function.arguments).objects
        query_results = self.apply_parent_strategy(query_results, **query_params)

        # compile the list of chunked documents and return it
        return compile_chunked_documents(
            query_results, named_vector=query_params["target_vector"]
        )


class SimilaritySearcher(AbstractSearcher):

    def create_query_params(self, query_text: str, **kwargs) -> dict[str, Any]:
        return super().create_query_params(query=query_text, **kwargs)

    def _conduct_search(self, collection: Collection, **kwargs) -> Callable:
        return collection.query.near_text


class VectorSimilaritySearcher(AbstractSearcher):

    def create_query_params(
        self, query_embedding: list[float], **kwargs
    ) -> dict[str, Any]:
        return super().create_query_params(near_vector=query_embedding, **kwargs)

    def _conduct_search(self, collection: Collection) -> Callable:
        return collection.query.near_vector


class HybridSearcher(AbstractSearcher):

    def create_query_params(self, query_text: str, **kwargs) -> dict[str, Any]:
        return super().create_query_params(query=query_text, **kwargs)

    def _conduct_search(self, collection: Collection) -> Callable:
        return collection.query.hybrid
