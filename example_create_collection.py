from genie_flow_invoker.invoker.weaviate import WeaviatePersistor, WeaviateClientFactory

client_factory = WeaviateClientFactory(
    config = {
        "http_host": "localhost",
        "http_port": 8080,
        "http_secure": False,
        "grpc_host": "localhost",
        "grpc_port": 50051,
        "grpc_secure": False,
    }
)

persistor = WeaviatePersistor(
    client_factory=client_factory,
    processor_params={
        "collection_name": "ExampleCollection",
    },
)
persistor.create_collection({})

client_factory._client.close()
