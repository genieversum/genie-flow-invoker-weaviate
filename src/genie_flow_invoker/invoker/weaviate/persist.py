from genie_flow_invoker.doc_proc import ChunkedDocument
from genie_flow_invoker.invoker.weaviate import WeaviateClientFactory
from weaviate.collections import Collection
from weaviate.classes.config import Configure, DataType, Property


def _compile_properties(params: dict):
    return [
        Property(name="filename", data_type=DataType.TEXT),
        Property(name="content", data_type=DataType.TEXT),
        Property(name="original_span_start", data_type=DataType.INT),
        Property(name="original_span_end", data_type=DataType.INT),
        Property(name="hierarchy_level", data_type=DataType.INT),
        Property(name="document_metadata", data_type=DataType.OBJECT),
    ]


def _compile_multi_tenancy(params: dict):
    config = dict(
        enabled=True,
        auto_tenant_creation=True,
        auto_tenant_activation=True,
    )
    config.update(params.get("multi_tenancy", {}))
    return Configure.multi_tenancy(**config)


def _compile_named_vectors(params: dict):
    Configure.NamedVectors.
    return params.get("named_vectors", [])

class WeaviatePersistor:

    def __init__(self, client_factory: WeaviateClientFactory, persist_params: dict) -> None:
        self.client_factory = client_factory

        self.base_persist_params = dict(
            collection_name=persist_params.get("collection_name", None),
            tenant_name=persist_params.get("tenant_name", None),
            operation_level=persist_params.get("operation_level", None),
        )

    def create_collection(self, persist_params: dict) -> Collection:
        params = self.base_persist_params.copy()
        params.update(persist_params)

        with self.client_factory as client:
            client.collections.create(
                params.get("collection_name"),
                properties=
                multi_tenancy_config=Configure.multi_tenancy(
                    enabled=True,
                ),
            )

    def persist_document(self, document: ChunkedDocument) -> None:
        for chunk in document.chunk_iterator()