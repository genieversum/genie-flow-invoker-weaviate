from collections import defaultdict
from typing import Optional

from genie_flow_invoker.doc_proc import ChunkedDocument, DocumentChunk
from genie_flow_invoker.invoker.weaviate import WeaviateClientFactory
from loguru import logger
from weaviate.collections import Collection
from weaviate.classes.config import Configure, DataType, Property, ReferenceProperty
from weaviate.collections.classes.internal import Object


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


def _compile_object(
        chunk: DocumentChunk,
        collection_name: str,
        filename: str,
        document_metadata: dict,
) -> Object:
    return Object(
        collection=collection_name,
        uuid=chunk.chunk_id,

        references={"parent": chunk.parent_id},
        me
    )

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

    def create_tenant(self, collection_name: str, tenant_name: str, idempotent: bool = False):
        """
        Create a new tenant for a collection with a given name. If a tenant already exists,
        with the given tenant name, a KeyError is raised - unless idempotent is set to True
        in which case the creation is silently ignored.

        :param collection_name: name of the collection to add to
        :param tenant_name: the name of the tenant to add
        :param idempotent: boolean indicating to accept creation of an already existing tenant.
                           Defaults to False
        """
        with self.client_factory as client:
            if not client.collections.exists(collection_name):
                raise KeyError(f"Collection {collection_name} does not exist")

            collection = client.collections.get(collection_name)
            if collection.tenants.exists(tenant_name):
                if idempotent:
                    return
                raise KeyError(f"Tenant {tenant_name} already exists")

            collection.tenants.create([tenant_name])

    def persist_document(
            self,
            document: ChunkedDocument,
            collection_name: str,
            tenant_name: Optional[str] = None,
    ) -> None:
        """
        Persist a given chunked document into a collection with the given name and potentially
        into a tenant with the given name.

        The hierarchy of the chunks in this document is retained and the document filename and
        other metadata is persisted with each and every Object.

        :param document: the `ChunkedDocument` to persist
        :param collection_name: the name of the collection to store it into
        :param tenant_name: an Optional name of a tenant to store the document into.
        :return: None
        """
        chunk_index = defaultdict(list)
        for chunk in document.chunks:
            chunk_index[chunk.hierarchy_level].append(chunk)

        with self.client_factory as client:
            logger.debug(
                "connecting to collection '{collection_name}'",
                collection_name=collection_name,
            )
            collection = client.collections.get(collection_name)
            if tenant_name is not None:
                if collection.tenants.exists(tenant_name):
                    logger.debug(
                        "connecting to tenant '{tenant_name}' "
                        "within collection '{collection_name}'",
                        collection_name=collection_name,
                        tenant_name=tenant_name,
                    )
                    collection = collection.with_tenant(tenant_name)
                else:
                    logger.error(
                        "tenant with name {tenant_name} does not exist "
                        "in collection {collection_name}",
                        tenant_name=tenant_name,
                        collection_name=collection_name,
                    )
                    raise KeyError(
                        f"Tenant {tenant_name} does not exist "
                        f"in collection {collection_name}")

        logger.info(
            "Connected to collection 'collection_name', persisting {nr_chunks} chunks, "
             "for file '{filename}'",
            collection_name=collection.name,
            nr_chunks=len(document.chunks),
            filename=document.filename,
        )

        # making sure we add the chunks from top to bottom
        for hierarchy_level, chunks in chunk_index.items():
            logger.debug(
                "persisting {nr_chunks} chunk(s) at hierarchy level {hierarchy_level}",
                nr_chunks=len(chunks),
                hierarchy_level=hierarchy_level,
            )
            for chunk in chunks:
                properties = {
                    "filename": document.filename,
                    "content": chunk.content,
                    "original_span_start": chunk.original_span[0],
                    "original_span_end": chunk.original_span[1],
                    "hierarchy_level": chunk.hierarchy_level,
                    "document_metadata": document.metadata,
                }
                references = {"parent": chunk.parent_id} if chunk.parent_id else None,
                if not collection.data.exists(chunk.chunk_id):
                    logger.debug(
                        "inserting chunk with id {chunk_id}",
                        chunk_id=chunk.chunk_id,
                    )
                    collection.data.insert(
                        uuid=chunk.chunk_id,
                        properties=properties,
                        references=references,
                        vector=chunk.embedding,
                    )
                else:
                    logger.debug(
                        "replacing chunk with id {chunk_id}",
                        chunk_id=chunk.chunk_id,
                    )
                    collection.data.replace(
                        uuid=chunk.chunk_id,
                        properties=properties,
                        references=references,
                        vector=chunk.embedding,
                    )
