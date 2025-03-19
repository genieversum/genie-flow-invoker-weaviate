import json
import uuid

from genie_flow_invoker.invoker.weaviate import (
    WeaviateDeleteChunkInvoker,
    WeaviateDeleter,
    WeaviateDeleteChunksRequest,
    WeaviateDeleteByFilenameInvoker,
    WeaviateDeleteByFilenameRequest,
    WeaviateDeleteByFilterInvoker,
    WeaviateDeleteByFilterRequest,
    WeaviateDeleteTenantInvoker,
    WeaviateDeleteMessage,
    WeaviateDeleteErrorResponse,
    TenantNotFoundException,
    CollectionNotFoundException,
    WeaviateDeleteCollectionInvoker,
)
import pytest

from genie_flow_invoker.invoker.weaviate.exceptions import NoTenantProvided


@pytest.fixture
def delete_return_result():
    return {
        "matches": 123,
        "failed": 1,
        "successful": 122,
    }


def test_invoke_delete_chunk(
    weaviate_client_factory, delete_return_result, monkeypatch
):
    monkeypatch.setattr(
        WeaviateDeleter,
        "delete_chunks_by_id",
        lambda *args, **kwargs: delete_return_result,
    )
    invoker = WeaviateDeleteChunkInvoker(weaviate_client_factory, {})
    delete_request = WeaviateDeleteChunksRequest(
        collection_name="SimpleCollection",
        chunk_id=[str(uuid.uuid4()), str(uuid.uuid4())],
    )
    result_json = invoker.invoke(delete_request.model_dump_json())
    result = json.loads(result_json)
    assert result["matches"] == 123
    assert result["failed"] == 1
    assert result["successful"] == 122


def test_invoke_delete_by_filename(
    weaviate_client_factory, delete_return_result, monkeypatch
):
    monkeypatch.setattr(
        WeaviateDeleter,
        "delete_chunks_by_filename",
        lambda *args, **kwargs: delete_return_result,
    )
    invoker = WeaviateDeleteByFilenameInvoker(weaviate_client_factory, {})
    delete_request = WeaviateDeleteByFilenameRequest(
        collection_name="SimpleCollection",
        filename="some-filename.txt",
    )
    result_json = invoker.invoke(delete_request.model_dump_json())
    result = json.loads(result_json)
    assert result["matches"] == 123
    assert result["failed"] == 1
    assert result["successful"] == 122


def test_invoker_delete_by_filter(
    weaviate_client_factory, delete_return_result, monkeypatch
):
    monkeypatch.setattr(
        WeaviateDeleter,
        "delete_by_filter",
        lambda *args, **kwargs: delete_return_result,
    )
    invoker = WeaviateDeleteByFilterInvoker(weaviate_client_factory, {})
    delete_request = WeaviateDeleteByFilterRequest(
        collection_name="SimpleCollection",
        having_any={
            "some-attr": 12,
        },
        having_all={
            "another-attr": "aap",
        },
    )
    result_json = invoker.invoke(delete_request.model_dump_json())
    result = json.loads(result_json)
    assert result["matches"] == 123
    assert result["failed"] == 1
    assert result["successful"] == 122


def test_invoke_delete_tenant_no_name(
    weaviate_client_factory, delete_return_result, monkeypatch
):
    class MockExceptionRaiser:
        def __init__(self, *args, **kwargs):
            raise NoTenantProvided(
                collection_name="SimpleCollection",
                tenant_name=None,
                message="Some Error description",
            )

    monkeypatch.setattr(
        WeaviateDeleter,
        "delete_tenant",
        MockExceptionRaiser,
    )
    invoker = WeaviateDeleteTenantInvoker(weaviate_client_factory, {})
    delete_request = WeaviateDeleteMessage(
        collection_name="SimpleCollection",
    )

    result_json = invoker.invoke(delete_request.model_dump_json())
    result = WeaviateDeleteErrorResponse.model_validate_json(result_json)

    assert result.collection_name == "SimpleCollection"
    assert result.tenant_name is None
    assert result.error_code == "NoTenantProvided"
    assert result.error == "Some Error description"


def test_invoke_delete_tenant_not_found(
    weaviate_client_factory, delete_return_result, monkeypatch
):
    class MockExceptionRaiser:
        def __init__(self, *args, **kwargs):
            raise TenantNotFoundException(
                collection_name="SimpleCollection",
                tenant_name="SomeTenant",
                message="That tenant does not exist",
            )

    monkeypatch.setattr(
        WeaviateDeleter,
        "delete_tenant",
        MockExceptionRaiser,
    )
    invoker = WeaviateDeleteTenantInvoker(weaviate_client_factory, {})
    delete_request = WeaviateDeleteMessage(
        collection_name="SimpleCollection",
        tenant_name="SomeTenant",
    )
    result_json = invoker.invoke(delete_request.model_dump_json())
    result = WeaviateDeleteErrorResponse.model_validate_json(result_json)

    assert result.collection_name == "SimpleCollection"
    assert result.tenant_name == "SomeTenant"
    assert result.error_code == "TenantNotFoundException"
    assert result.error == "That tenant does not exist"


def test_invoke_delete_collection(weaviate_client_factory, monkeypatch):
    class MockExceptionRaiser:
        def __init__(self, *args, **kwargs):
            raise CollectionNotFoundException(
                collection_name="SimpleCollection",
                tenant_name=None,
                message="That collection does not exist",
            )

    monkeypatch.setattr(
        WeaviateDeleter,
        "delete_collection",
        MockExceptionRaiser,
    )
    invoker = WeaviateDeleteCollectionInvoker(weaviate_client_factory, {})
    delete_request = WeaviateDeleteMessage(
        collection_name="SimpleCollection",
    )
    result_json = invoker.invoke(delete_request.model_dump_json())
    result = WeaviateDeleteErrorResponse.model_validate_json(result_json)

    assert result.collection_name == "SimpleCollection"
    assert result.tenant_name is None
    assert result.error_code == "CollectionNotFoundException"
    assert result.error == "That collection does not exist"
