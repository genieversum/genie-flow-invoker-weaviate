from genie_flow_invoker.invoker.weaviate import WeaviatePersistor


def test_persist_same(weaviate_client_factory, chunked_document):
    params = {
        "collection_name": "SimpleCollection",
        "idempotent": True,
    }
    persistor = WeaviatePersistor(weaviate_client_factory, params)

    collection_name, tenant_name, nr_inserts, nr_replaces = persistor.persist_document(
        chunked_document,
        "SimpleCollection",
        None,
    )
    assert collection_name == "SimpleCollection"
    assert tenant_name is None
    assert nr_inserts == 0
    assert nr_replaces == 2


def test_persist_other(weaviate_client_factory, other_chunked_document):
    params = {
        "collection_name": "SimpleCollection",
    }
    persistor = WeaviatePersistor(weaviate_client_factory, params)

    collection_name, tenant_name, nr_inserts, nr_replaces = persistor.persist_document(
        other_chunked_document,
        "SimpleCollection",
        None,
    )
    assert collection_name == "SimpleCollection"
    assert tenant_name is None
    assert nr_inserts == 2
    assert nr_replaces == 0

    with weaviate_client_factory as client:
        collection = client.collections.get("SimpleCollection")
    assert len(collection.query.fetch_objects()) == 4
