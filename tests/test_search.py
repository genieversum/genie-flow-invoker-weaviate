import uuid

from genie_flow_invoker.invoker.weaviate import SimilaritySearcher
from weaviate.collections.classes.filters import (
    _FilterAnd,
    _FilterOr,
    _FilterValue,
    _Operator,
)

from genie_flow_invoker.invoker.weaviate.properties import create_flat_name


def test_similarity_query_params_query(weaviate_client_factory):
    searcher = SimilaritySearcher(
        weaviate_client_factory, dict(collection_name="SimpleCollection")
    )
    query_params = searcher.create_query_params("my query")

    assert query_params["query"] == "my query"
    assert (
        query_params["collection"].query_results
        == weaviate_client_factory.collections_results["SimpleCollection"]
    )
    assert query_params["target_vector"] == "default"
    assert query_params["include_vector"] == False
    assert query_params["method"] == "cosine"


def test_similarity_query_params_target_vector(weaviate_client_factory):
    searcher = SimilaritySearcher(
        weaviate_client_factory,
        dict(
            collection_name="SimpleCollection",
            vector_name="some_other_vector",
        ),
    )
    query_params = searcher.create_query_params("my query")

    assert query_params["query"] == "my query"
    assert (
        query_params["collection"].query_results
        == weaviate_client_factory.collections_results["SimpleCollection"]
    )
    assert query_params["target_vector"] == "some_other_vector"
    assert query_params["include_vector"] == False
    assert query_params["method"] == "cosine"


def test_similarity_query_params_include_vector(weaviate_client_factory):
    searcher = SimilaritySearcher(
        weaviate_client_factory,
        dict(
            collection_name="SimpleCollection",
            include_vector=True,
        ),
    )
    query_params = searcher.create_query_params("my query")

    assert query_params["query"] == "my query"
    assert (
        query_params["collection"].query_results
        == weaviate_client_factory.collections_results["SimpleCollection"]
    )
    assert query_params["target_vector"] == "default"
    assert query_params["include_vector"] == True
    assert query_params["method"] == "cosine"


def test_similarity_query_params_method(weaviate_client_factory):
    searcher = SimilaritySearcher(
        weaviate_client_factory,
        dict(
            collection_name="SimpleCollection",
            method="manhattan",
        ),
    )
    query_params = searcher.create_query_params("my query")

    assert query_params["query"] == "my query"
    assert (
        query_params["collection"].query_results
        == weaviate_client_factory.collections_results["SimpleCollection"]
    )
    assert query_params["target_vector"] == "default"
    assert query_params["include_vector"] == False
    assert query_params["method"] == "manhattan"


def test_similarity_query_params_tenant(weaviate_client_factory):
    searcher = SimilaritySearcher(
        weaviate_client_factory,
        dict(
            collection_name="SimpleCollection",
            tenant_name="TenantSimpleCollection",
        ),
    )
    query_params = searcher.create_query_params("my query")

    assert query_params["query"] == "my query"
    assert (
        query_params["collection"].query_results
        == weaviate_client_factory.collections_results["SimpleCollection"]
    )
    assert (
        query_params["collection"].name == "SimpleCollection / TenantSimpleCollection"
    )
    assert query_params["target_vector"] == "default"
    assert query_params["include_vector"] == False
    assert query_params["method"] == "cosine"


def test_similarity_query_params_extra(weaviate_client_factory):
    searcher = SimilaritySearcher(
        weaviate_client_factory,
        dict(
            collection_name="SimpleCollection",
            some_extra_param="some_extra_value",
        ),
    )
    query_params = searcher.create_query_params("my query")
    assert "some_extra_param" not in query_params


def test_similarity_query_params_top_horizon(weaviate_client_factory):
    searcher = SimilaritySearcher(
        weaviate_client_factory,
        dict(
            collection_name="SimpleCollection",
            top=16,
            horizon=0.7,
        ),
    )
    query_params = searcher.create_query_params("my query")

    assert query_params["limit"] == 16
    assert query_params["distance"] == 0.7


def test_similarity_query_params_parent(weaviate_client_factory):
    searcher = SimilaritySearcher(
        weaviate_client_factory,
        dict(
            collection_name="SimpleCollection",
            parent_strategy="replace",
        ),
    )
    query_params = searcher.create_query_params("my query")

    assert len(query_params["return_references"]) == 1
    assert query_params["return_references"][0].link_on == "parent"


def test_similarity_query_params_has_all(weaviate_client_factory):
    searcher = SimilaritySearcher(
        weaviate_client_factory,
        dict(
            collection_name="SimpleCollection",
            having_all={"some_property": 42, "other_property <": 0},
        ),
    )
    query_params = searcher.create_query_params("my query")
    assert type(query_params["filters"]) == _FilterAnd
    assert len(query_params["filters"].filters) == 2
    assert query_params["filters"].filters[0].value == 42
    assert query_params["filters"].filters[0].operator == _Operator.EQUAL
    assert query_params["filters"].filters[0].target == create_flat_name("some_property")
    assert query_params["filters"].filters[1].value == 0
    assert query_params["filters"].filters[1].operator == _Operator.LESS_THAN
    assert query_params["filters"].filters[1].target == create_flat_name("other_property")


def test_similarity_query_params_has_any(weaviate_client_factory):
    searcher = SimilaritySearcher(
        weaviate_client_factory,
        dict(
            collection_name="SimpleCollection",
            having_all={"some_property": 42, "other_property <": 0},
        ),
    )
    query_params = searcher.create_query_params("my query")
    assert type(query_params["filters"]) == _FilterAnd
    assert len(query_params["filters"].filters) == 2
    assert query_params["filters"].filters[0].value == 42
    assert query_params["filters"].filters[0].operator == _Operator.EQUAL
    assert query_params["filters"].filters[0].target == create_flat_name("some_property")
    assert query_params["filters"].filters[1].value == 0
    assert query_params["filters"].filters[1].operator == _Operator.LESS_THAN
    assert query_params["filters"].filters[1].target == create_flat_name("other_property")


def test_similarity_query_params_has_all_and_any(weaviate_client_factory):
    searcher = SimilaritySearcher(
        weaviate_client_factory,
        dict(
            collection_name="SimpleCollection",
            having_all={"some_property": 42, "other_property <": 0},
            having_any={"third_property": 24, "other_property >=": -99},
        ),
    )
    query_params = searcher.create_query_params("my query")
    assert type(query_params["filters"]) == _FilterAnd
    assert len(query_params["filters"].filters) == 2
    assert type(query_params["filters"].filters[0]) == _FilterAnd
    assert type(query_params["filters"].filters[1]) == _FilterOr


def test_similarity_query_params_level(weaviate_client_factory):
    searcher = SimilaritySearcher(
        weaviate_client_factory,
        dict(
            collection_name="SimpleCollection",
            operation_level=2,
        ),
    )
    query_params = searcher.create_query_params("my query")
    assert type(query_params["filters"]) == _FilterValue
    assert query_params["filters"].value == 2
    assert query_params["filters"].operator == _Operator.EQUAL
    assert query_params["filters"].target == "hierarchy_level"


def test_similarity_search(weaviate_client_factory):
    searcher = SimilaritySearcher(
        weaviate_client_factory,
        dict(
            collection_name="SimpleCollection",
        ),
    )
    results = searcher.search(query_text="my query")
    assert len(results) == 1

    result = results[0]
    assert result.filename == "some_file.txt"
    assert result.document_metadata["language"] == "en"
    assert result.document_metadata["source"] == "pdf"
    assert len(result.chunks) == 2

    chunk = result.chunks[1]
    assert chunk.chunk_id == str(uuid.uuid3(uuid.NAMESPACE_OID, "first document"))
    assert chunk.content == "Hello World"
    assert chunk.original_span == (0, 42)
    assert chunk.hierarchy_level == 1
    assert chunk.parent_id == str(uuid.uuid3(uuid.NAMESPACE_OID, "second document"))
    assert chunk.embedding == [3.14] * 12
