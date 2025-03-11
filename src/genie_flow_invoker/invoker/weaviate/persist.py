from collections import defaultdict
from typing import Optional

from genie_flow_invoker.doc_proc import ChunkedDocument
from genie_flow_invoker.invoker.weaviate import WeaviateClientFactory
from loguru import logger
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
    config = dict(
        enabled=True, auto_tenant_creation=False, auto_tenant_activation=False
    )
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
    return params.get(
        "named_vectors",
        [
            getattr(Configure.NamedVectors, value["vectorizer"])(
                name=key,
                source_properties=value["source_properties"],
                vector_index_config=Configure.VectorIndex.hnsw(),
            )
            for key, value in config.items()
        ],
    )


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

    def __init__(
        self, client_factory: WeaviateClientFactory, persist_params: dict
    ) -> None:
        self.client_factory = client_factory

        self.base_persist_params = dict(
            collection_name=persist_params.get("collection_name", None),
            tenant_name=persist_params.get("tenant_name", None),
        )

    def compile_collection_tenant_names(
        self,
        collection_name: Optional[str],
        tenant_name: Optional[str],
    ) -> tuple[str, Optional[str]]:
        result = (
            collection_name or self.base_persist_params.get("collection_name"),
            tenant_name or self.base_persist_params.get("tenant_name"),
        )
        if result[0] is None:
            raise ValueError("collection_name is required")
        return result

    def create_collection(
        self, persist_params: dict, omnipotent: bool = False
    ) -> Collection:
        """
        Create a new collection with the given name and the configuration that is compiled from
        the given persist_params. Raises a ValueError when a collection with that name
        already exists - unless omnipotent is set to True.

        :param persist_params: the configuration parameters to create the collection with
        :param omnipotent: a boolean indicating to ignore creating already existing collections.
                           Defauts to False
        :return: the newly created collection
        """
        params = self.base_persist_params.copy()
        params.update(persist_params)

        collection_name: Optional[str] = params.get("collection_name", None)
        if collection_name is None:
            raise ValueError("No collection name specified")

        with self.client_factory as client:
            if client.collections.exists(collection_name):
                if omnipotent:
                    logger.warning(
                        "Skipping creation of collection '{collection_name}' that already exists.",
                        collection_name=collection_name,
                    )
                    return client.collections.get(collection_name)

                logger.error(
                    "Cannot create collection with name '{collection_name}' "
                    "because it already exists.",
                    collection_name=collection_name,
                )
                raise ValueError("Collection {collection_name} already exists")

            return client.collections.create(
                name=collection_name,
                properties=_compile_properties(params),
                multi_tenancy_config=_compile_multi_tenancy(params),
                references=_compile_cross_references(params),
            )

    def create_tenant(
        self,
        collection_name: str,
        tenant_name: str,
        idempotent: bool = False,
    ) -> Collection:
        """
        Create a new tenant for a collection with a given name. If a tenant already exists,
        with the given tenant name, a ValueError is raised - unless idempotent is set to True
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
                    logger.warning(
                        "Skipping creation of tenant '{tenant_name}' that already exists.",
                        tenant_name=tenant_name,
                    )
                    return collection.with_tenant(tenant_name)
                raise ValueError(f"Tenant {tenant_name} already exists")

        collection.tenants.create([tenant_name])
        return collection.collections.get(collection_name).with_tennant(tenant_name)

    def get_or_create(self, params: dict) -> Collection:
        collection_name: Optional[str] = params.get("collection_name", None)
        if collection_name is None:
            raise ValueError("No collection name specified")

        with self.client_factory as client:
            if not client.collections.exists(collection_name):
                collection = self.create_collection(params)
            else:
                collection = client.collections.get(collection_name)

        tenant_name: Optional[str] = params.get("tenant_name", None)
        if tenant_name is None:
            return collection

        with self.client_factory as client:
            if not collection.tenants.exists(tenant_name):
                return self.create_tenant(collection_name, tenant_name)
            else:
                return collection.with_tennant(tenant_name)

    def persist_document(
        self,
        document: ChunkedDocument,
        collection_name: Optional[str] = None,
        tenant_name: Optional[str] = None,
    ) -> tuple[str, Optional[str], int, int]:
        """
        Persist a given chunked document into a collection with the given name and potentially
        into a tenant with the given name.

        The hierarchy of the chunks in this document is retained and the document filename and
        other metadata is persisted with each and every Object.

        :param document: the `ChunkedDocument` to persist
        :param collection_name: the name of the collection to store it into
        :param tenant_name: an Optional name of a tenant to store the document into.
        :return: tuple of the used collection_name and tenant_name, nr_inserted and nr_replaces,
                respectively the number of inserted and replaced chunks
        """
        collection_name, tenant_name = self.compile_collection_tenant_names(
            collection_name, tenant_name
        )

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
            if not collection.tenants.exists(tenant_name):
                logger.error(
                    "tenant '{tenant_name}' does not exist in collection '{collection_name}'",
                    tenant_name=tenant_name,
                    collection_name=collection_name,
                )
                raise KeyError(
                    f"Tenant {tenant_name} does not exist in collection {collection_name}"
                )

            logger.debug(
                "connecting to tenant '{tenant_name}' "
                "within collection '{collection_name}'",
                collection_name=collection_name,
                tenant_name=tenant_name,
            )
            collection = collection.with_tenant(tenant_name)

        logger.info(
            "Connected to collection '{collection_name}', persisting {nr_chunks} chunks, "
            "for file '{filename}'",
            collection_name=collection.name,
            nr_chunks=len(document.chunks),
            filename=document.filename,
        )

        # making sure we add the chunks from top to bottom
        nr_inserted = 0
        nr_replaced = 0
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
                    "document_metadata": document.document_metadata,
                }
                references = {"parent": chunk.parent_id} if chunk.parent_id else None

                if not collection.data.exists(chunk.chunk_id):
                    logger.debug(
                        "inserting chunk with id {chunk_id}", chunk_id=chunk.chunk_id
                    )
                    collection.data.insert(
                        uuid=chunk.chunk_id,
                        properties=properties,
                        references=references,
                        vector=chunk.embedding,
                    )
                    nr_inserted += 1
                else:
                    logger.debug(
                        "replacing chunk with id {chunk_id}", chunk_id=chunk.chunk_id
                    )
                    collection.data.replace(
                        uuid=chunk.chunk_id,
                        properties=properties,
                        references=references,
                        vector=chunk.embedding,
                    )
                    nr_replaced += 1

        return collection_name, tenant_name, nr_inserted, nr_replaced
