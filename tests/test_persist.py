from genie_flow_invoker.invoker.weaviate import WeaviatePersistor


def test_persist_collection(weaviate_client_factory, chunked_document):
    params = {
        "collection_name": "SimpleCollection",
    }
    persistor = WeaviatePersistor(weaviate_client_factory, params)

    nr_inserts, nr_replaces = persistor.persist_document(
        chunked_document,
        "SimpleCollection",
        None,
    )
    print(nr_inserts, nr_replaces)