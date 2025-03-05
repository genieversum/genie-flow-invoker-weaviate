from abc import ABC
from typing import Optional, Any

from weaviate.classes.query import MetadataQuery, Filter, QueryReference, Metrics
from weaviate.collections.classes.filters import _Filters
from weaviate.collections.classes.internal import Object
from weaviate.proto.v1.search_get_pb2 import SearchReply

from genie_flow_invoker.invoker.weaviate import WeaviateClientFactory
from genie_flow_invoker.invoker.weaviate.model import WeaviateSimilaritySearchRequest


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


def apply_parent_strategy(
        parent_strategy: str,
    query_results: list[Object],
) -> list[Object]:
    """
    Apply a parent strategy to the query results. If the parent strategy is "replace",
    then only the parents are returned - in the same order as their children. Duplicate
    parents are removed. If the strategy is "include" the parents are added to the list
    of children, deduplicating the parents by having a parent follow the child that comes
    first in order.

    :param parent_strategy: a string representing the parent strategy
    :param query_results: the list of objects returned from the query
    :return: a list of objects with the parent strategy applied
    """
    seen_parents = set()

    if parent_strategy == "replace":
        # return a deduplicated list of parents, retaining the order
        parents = list()
        for child in query_results:
            for parent in child.references["parent"]:
                if parent.id not in seen_parents:
                    parents.append(parent)
                    seen_parents.add(parent.id)
        return parents

    # return a combined list of children and their parents, making sure that
    # parents are de-duplicated
    combined = list()
    for child in query_results:
        combined.append(child)
        for parent in child.references["parent"]:
            if parent.id not in seen_parents:
                combined.append(parent)
                seen_parents.add(parent.id)
    return combined


class AbstractSimilaritySearcher(ABC):

    def __init__(self, client_factory: WeaviateClientFactory, query_params: dict):
        self.client_factory = client_factory

        def cast_or_none(dictionary: dict, key: str, data_type: type):
            try:
                return data_type(dictionary[key])
            except (KeyError, TypeError):
                return None

        self.collection_name = query_params.get("collection")
        self.tenant_name = query_params.get("tenant", None)

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

    def _calculate_operation_level(self, operation_level: Optional[int]) -> Optional[int]:
        if operation_level is None:
            return None
        if operation_level >= 0:
            return operation_level

        # if we look at a negative level, we need to count from the highest number
        response = self.collection.aggregate.over_all(
            return_metrics=Metrics("hierarchy_level").integer(sum_=False, maximum=True, minimum=False)
        )
        max_level = response.properties["hierarchy_level"].maximum
        return max_level + operation_level + 1

    @property
    def collection(self):
        with self.client_factory as client:
            collection = client.collections.get(self.collection_name)
            if self.tenant_name:
                collection = collection.tenants.get(self.tenant_name)
        return collection

    @property
    def query(self):
        return self.collection.query

    def create_query_params(self, **kwargs) -> dict[str, Any]:
        # start with the base query parameters
        query_params =  self.base_query_params.copy()

        # incorporate any query parameters passed
        query_params.update(**kwargs)

        # if we have a filter, include that into the query parameters
        query_params["filter"] = compile_filter(query_params)

        # if we need the parents, pull in the references too
        if query_params["parent_strategy"] is not None:
            query_params["return_references"] = [
                QueryReference(link_on="parent_id")
            ]

        # if we need to operate at a certain level, filter on that level
        if query_params["operation_level"] is not None:
            operation_level = self._calculate_operation_level(query_params["operation_level"])
            hierarchy_filter = Filter.by_property("hierarchy_level").equal(operation_level)
            if query_params["filter"] is not None:
                query_params["filter"] &= hierarchy_filter
            else:
                query_params["filter"] = hierarchy_filter

        return {
            parameter: query_params[parameter]
            for parameter in [
                "vector_name",
                "include_vector",
                "method",
                "limit",
                "distance",
                "filter",
                "return_references",
            ]
            if parameter in query_params
        }


class SimilaritySearcher(AbstractSimilaritySearcher):

    def search(self, query_text: str) -> list[Object]:
        query_params = self.create_query_params()
        query_params["query"] = query_text
        children = self.query.near_text(**query_params)

        parent_strategy = self.base_query_params["parent_strategy"]
        if parent_strategy is not None:
            return apply_parent_strategy(parent_strategy, children)
        return children
