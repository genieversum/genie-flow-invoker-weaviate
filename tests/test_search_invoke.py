import json
import uuid

from genie_flow_invoker.doc_proc import ChunkedDocument
from genie_flow_invoker.invoker.weaviate import (
    WeaviateSimilaritySearchInvoker,
    WeaviateVectorSimilaritySearchInvoker,
    WeaviateSimilaritySearchRequest,
    WeaviateSimilaritySearchRequestInvoker,
)


def test_search_invoke(weaviate_client_factory):
    invoker = WeaviateSimilaritySearchInvoker(
        weaviate_client_factory,
        dict(
            collection_name="SimpleCollection",
        ),
    )
    result_json = invoker.invoke("who killed Bambi")
    result = [ChunkedDocument.model_validate(r) for r in json.loads(result_json)]

    assert len(result) == 1

    result = result[0]
    assert len(result.chunks) == 2
    assert result.document_metadata["source"] == "pdf"

    chunk = result.chunks[1]
    assert chunk.chunk_id == str(uuid.uuid3(uuid.NAMESPACE_OID, "first document"))
    assert chunk.hierarchy_level == 1
    assert chunk.content == "Hello World"


def test_request_search_invoke(weaviate_client_factory):
    invoker = WeaviateSimilaritySearchRequestInvoker(weaviate_client_factory, dict())
    search_request = WeaviateSimilaritySearchRequest(
        filename="some_file.txt",
        collection_name="SimpleCollection",
        vector_name="low_space",
        include_vector=True,
        method="manhattan",
        parent_strategy="replace",
        top=16,
        auto_limit=2,
        operation_level=-1,
        query_embedding=[2.27] * 12,
    )
    result_json = invoker.invoke(search_request.model_dump_json())
    result = [ChunkedDocument.model_validate(r) for r in json.loads(result_json)]
    assert len(result) == 1
    assert len(result[0].chunks) == 1
    assert result[0].chunks[0].content == "Hello Parent"


def test_request_search_filter_invoke(weaviate_client_factory):
    invoker = WeaviateSimilaritySearchRequestInvoker(weaviate_client_factory, dict())
    search_request = WeaviateSimilaritySearchRequest(
        filename="some_file.txt",
        collection_name="SimpleCollection",
        having_any={
            "custom_property.some_property": "this is a custom property",
        },
        query_embedding=[2.27] * 12,
    )
    result_json = invoker.invoke(search_request.model_dump_json())
    result = [ChunkedDocument.model_validate(r) for r in json.loads(result_json)]
    assert len(result) == 1
    assert len(result[0].chunks) == 1
    assert result[0].chunks[0].content == "Hello World"


def test_request_search_filter_any_invoke(weaviate_client_factory):
    invoker = WeaviateSimilaritySearchRequestInvoker(weaviate_client_factory, dict())
    search_request = WeaviateSimilaritySearchRequest(
        filename="some_file.txt",
        collection_name="SimpleCollection",
        having_any={
            "custom_property.some_property": "this is a custom property",
            "document_metadata.source": "pdf",
        },
        query_embedding=[2.27] * 12,
    )
    result_json = invoker.invoke(search_request.model_dump_json())
    result = [ChunkedDocument.model_validate(r) for r in json.loads(result_json)]
    assert len(result) == 1
    assert len(result[0].chunks) == 2


def test_request_search_filter_all_invoke(weaviate_client_factory):
    invoker = WeaviateSimilaritySearchRequestInvoker(weaviate_client_factory, dict())
    search_request = WeaviateSimilaritySearchRequest(
        filename="some_file.txt",
        collection_name="SimpleCollection",
        having_all={
            "custom_property.some_property": "this is a custom property",
            "document_metadata.language": "nl",
        },
        query_embedding=[2.27] * 12,
    )
    result_json = invoker.invoke(search_request.model_dump_json())
    result = [ChunkedDocument.model_validate(r) for r in json.loads(result_json)]
    assert len(result) == 0
