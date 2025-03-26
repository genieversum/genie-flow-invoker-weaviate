from conftest import MockQuery
from genie_flow_invoker.invoker.weaviate import (
    WeaviatePersistInvoker,
    WeaviatePersistenceResponse,
    WeaviatePersistenceRequest,
)


def test_persist_invocation(weaviate_client_factory, chunked_document):
    config = {
        "collection_name": "SimpleCollection",
        "tenant_name": None,
        "idempotent": True,
    }
    invoker = WeaviatePersistInvoker(weaviate_client_factory, config)

    request = WeaviatePersistenceRequest(
        document=chunked_document,
    )
    response_json = invoker.invoke(request.model_dump_json())
    response = WeaviatePersistenceResponse.model_validate_json(response_json)

    assert response.collection_name == "SimpleCollection"
    assert response.tenant_name is None
    assert response.nr_inserts == 0
    assert response.nr_replaces == 2
