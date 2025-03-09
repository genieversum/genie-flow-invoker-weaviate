from typing import Optional

from genie_flow_invoker.doc_proc import ChunkedDocument
from genie_flow_invoker.invoker.weaviate import WeaviateClientFactory
from weaviate.collections import Collection
from weaviate.classes.config import Configure, DataType, Property, ReferenceProperty


def _compile_properties(params: dict):
    """
    The standard properties defined for a Document Chunk. Additional properties can be added
    through a parameter `properties` that should contain a dictionary of property name with their
    value set to the name of the DataType.

    :param params: the configuration parameters, potentially containing a `properties` dictionary
                   containing additional properties to add
    :return: the properties configuration to use to create the collection
    """
    extra_properties = [
        Property(name=key, data_type=getattr(DataType, value.upper()))
        for key, value in params.get("properties", {}).items()
    ]
    return [
        Property(name="filename", data_type=DataType.TEXT),
        Property(name="content", data_type=DataType.TEXT),
        Property(name="original_span_start", data_type=DataType.INT),
        Property(name="original_span_end", data_type=DataType.INT),
        Property(name="hierarchy_level", data_type=DataType.INT),
        Property(name="document_metadata", data_type=DataType.OBJECT),
        *extra_properties,
    ]


def _compile_multi_tenancy(params: dict):
    """
    Compile the multi-tenancy configuration, defaulting to `enabled=True` and leaving the
    automatic properties to `False` ("best be explicit"). In the configuration, these properties
    can be overridden by setting the values in a property `multi_tenancy`, specifying the
    values to apply.

    :param params: the configuration parameters, potentially containing a `multy_tenancy` dictionary
    :return: the multy tenancy configuration settings
    """
    config = dict(enabled=True, auto_tenant_creation=False, auto_tenant_activation=False)
    config.update(params.get("multi_tenancy", {}))
    return Configure.multi_tenancy(**config)


def _compile_named_vectors(params: dict):
    """
    Create the named vector configuration. This defaults to a named vector called "default" using
    the "text2vec_huggingface" vectorizer and indexing property "content".

    This default can be overridden by passing a dictionary under the key "named_vectors", where
    `source_properties` and `vectorizer` are specified.

    :param params: the configuration parameters, potentially containing a `named_vectors` dictionary
    :return: the named vector configuration settings
    """
    config = {
        "default": {
            "source_properties": ["content"],
            "vectorizer": "text2vec_huggingface",
        }
    }
    config.update(params.get("named_vectors", {}))
    return params.get("named_vectors", [
        getattr(Configure.NamedVectors, value["vectorizer"])(
            name=key,
            source_properties=value["source_properties"],
            vector_index_config=Configure.VectorIndex.hnsw()
        )
        for key, value in config.items()
    ])


def _compile_cross_references(params: dict):
    """
    Create the configuration for cross-references. This defaults to cross-referencing the parent
    of an Object, via the `parent` property.

    This default cannot be overridden and params is only used to retrieve the collection name.
    :param params: ignored
    :return: the cross-references configuration settings
    """
    collection_name: str = params.get("collection_name")
    return [
        ReferenceProperty(
            name="parent",
            target_collection=collection_name,
        )
    ]


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

        collection_name: Optional[str] = params.get("collection_name", None)
        if collection_name is None:
            raise ValueError("No collection name specified")

        with self.client_factory as client:
            client.collections.create(
                name=collection_name,
                properties=_compile_properties(params),
                multi_tenancy_config=_compile_multi_tenancy(params),
                references=_compile_cross_references(params)
            )

    def create_tenant(self, collection_name: str, tenant_name: str):
        with self.client_factory as client:
            if not client.collections.exists(collection_name):
                raise KeyError(f"Collection {collection_name} does not exist")

            client.collections.get(collection_name).tenants.create([tenant_name])

    def persist_document(self, document: ChunkedDocument) -> None:
        for chunk in document.chunk_iterator()