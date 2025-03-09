from abc import ABC, abstractmethod
from inspect import signature
from typing import Optional, Any, Callable

from loguru import logger
from weaviate.collections import Collection
from weaviate.classes.query import Filter, QueryReference, Metrics
from weaviate.collections.classes.filters import _Filters
from weaviate.collections.classes.internal import Object

from genie_flow_invoker.doc_proc import ChunkedDocument, DocumentChunk
from src.genie_flow_invoker.invoker.weaviate import WeaviateClientFactory


def _create_attribute_filter(key: str, value: Any) -> _Filters | None:
    if " " in key:
        key_name, indicator = key.rsplit(" ", 1)
    else:
        key_name = key
        indicator = "="
    filter_by_property = Filter.by_property(key_name)
    match indicator:
        case "=":
            return filter_by_property.equal(value)
        case "!=":
            return filter_by_property.not_equal(value)
        case "<":
            return filter_by_property.less_than(value)
        case "<=":
            return filter_by_property.less_or_equal(value)
        case ">":
            return filter_by_property.greater_than(value)
        case ">=":
            return filter_by_property.greater_or_equal(value)
        case "@":
            return filter_by_property.contains_any(value)
        case _:
            raise ValueError(
                f"Got filter indicator '{indicator}' that is not supported"
            )


def compile_filter(query_params: dict) -> Optional[Filter]:
    query_filter = None

    if query_params["having_all"] is not None:
        query_filter = Filter.all_of(
            [
                _create_attribute_filter(key, value)
                for key, value in query_params["having_all"].items()
            ]
        )
    if query_params["having_any"] is not None:
        any_filter = Filter.any_of(
            [
                _create_attribute_filter(key, value)
                for key, value in query_params["having_any"].items()
            ]
        )
        if query_filter is not None:
            query_filter = query_filter & any_filter
        else:
            query_filter = any_filter

    return query_filter


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
    document_index: dict[str, ChunkedDocument] = dict()
    for o in query_results:
        properties = o.properties
        chunk = DocumentChunk(
            chunk_id=str(o.uuid),
            content=properties["content"],
            original_span=(
                properties["original_span_start"],
                properties["original_span_end"],
            ),
            hierarchy_level=properties["hierarchy_level"],
            parent_id=str(o.references["parent"][0].uuid) if o.references else None,
            embedding=o.vector[named_vector] if o.vector is not None else None,
        )
        filename = properties["filename"]
        try:
            document_index[filename].chunks.append(chunk)
        except KeyError:
            document_index[filename] = ChunkedDocument(
                filename=filename,
                document_metadata=properties["document_metadata"],
                chunks=[chunk],
            )
    return [document for document in document_index.values()]


def _calculate_operation_level(
    collection: Collection, operation_level: Optional[int]
) -> int:
    response = collection.aggregate.over_all(
        return_metrics=Metrics("hierarchy_level").integer(maximum=True),
    )
    return response.properties["hierarchy_level"].maximum + operation_level + 1


class AbstractSearcher(ABC):

    def __init__(self, client_factory: WeaviateClientFactory, query_params: dict):
        self.client_factory = client_factory

        def cast_or_none(dictionary: dict, key: str, data_type: type):
            try:
                return data_type(dictionary[key])
            except (KeyError, TypeError):
                return None

        self.base_query_params = dict(
            collection_name=query_params.get("collection_name", None),
            tenant_name=query_params.get("tenant_name", None),
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
        query_params = self.base_query_params.copy()
        query_params.update(**kwargs)

        if query_params.get("collection_name", None) in [None, ""]:
            logger.error("Missing collection name from query parameters")
            raise ValueError("Missing collection name from query parameters")

        with self.client_factory as client:
            collection = client.collections.get(query_params["collection_name"])
        if query_params.get("tenant_name", None) not in [None, ""]:
            collection = collection.tenants.get(query_params["tenant_name"])
        query_params["collection"] = collection

        translations = {
            "top": "limit",
            "horizon": "distance",
        }
        for genie_param, weaviate_param in translations.items():
            if genie_param in query_params:
                query_params[weaviate_param] = query_params[genie_param]

        # if a non-default vector is specified, set the target to it
        query_params["target_vector"] = (
            query_params["vector_name"]
            if query_params.get("vector_name", None) not in [None, ""]
            else "default"
        )

        # if we have a filter, include that into the query parameters
        query_params["filter"] = compile_filter(query_params)

        # if we need the parents, pull in the references too
        if query_params["parent_strategy"] is not None:
            query_params["return_references"] = [QueryReference(link_on="parent_id")]

        # if we need to operate at a certain level, filter on that level
        if query_params["operation_level"] is not None:
            operation_level = query_params["operation_level"]
            if operation_level < 0:
                operation_level = _calculate_operation_level(
                    collection, operation_level
                )
            hierarchy_filter = Filter.by_property("hierarchy_level").equal(
                operation_level
            )
            if query_params["filter"] is not None:
                query_params["filter"] &= hierarchy_filter
            else:
                query_params["filter"] = hierarchy_filter

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
        parent_strategy = self.base_query_params.get("parent_strategy", None)
        if "parent_strategy" in kwargs:
            parent_strategy = kwargs["parent_strategy"]
        if parent_strategy is None:
            return query_results

        seen_parents = set()
        if parent_strategy == "replace":
            # return a deduplicated list of parents, retaining the order
            parents = list()
            for child in query_results:
                for parent in child.references["parent"]:
                    if parent.uuid not in seen_parents:
                        parents.append(parent)
                        seen_parents.add(parent.uuid)
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
        collection = query_params["collection"]

        # bind the necessary arguments to the values in query_params
        search_function = self._conduct_search(collection)
        sig = signature(search_function)
        bound = sig.bind(**query_params)

        # conduct the search and apply the parent strategy
        query_results = search_function(**bound.arguments)
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
