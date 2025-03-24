import re
from collections import defaultdict
from typing import Optional, Any

from genie_flow_invoker.doc_proc import ChunkedDocument
from loguru import logger
from weaviate.exceptions import UnexpectedStatusCodeError

from genie_flow_invoker.invoker.weaviate.base import WeaviateClientProcessor
from weaviate.classes.config import (
    Configure,
    DataType,
    Property,
    ReferenceProperty,
)
from weaviate.collections import Collection


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


_METADATA_FIRST_CHAR = re.compile("[_A-Za-z]")
_METADATA_REMAINING_CHARS = re.compile("[_0-9A-Za-z]")
def _clean_metadata_key(key: str) -> str:
    if len(key) == 0:
        logger.error("Metadata contains empty key")
        raise ValueError("Metadata contains empty key")

    result = key[0] if _METADATA_FIRST_CHAR.match(key[0]) else "_"
    for c in key[1:]:
        result += c if _METADATA_REMAINING_CHARS.match(c) else "_"
    return result


def clean_nested_metadata_properties(metadata: Any) -> Any:
    if isinstance(metadata, dict):
        return {
            _clean_metadata_key(k): clean_nested_metadata_properties(v)
            for k, v in metadata.items()
        }
    if isinstance(metadata, list):
        return [clean_nested_metadata_properties(e) for e in metadata]
    if isinstance(metadata, tuple):
        return (clean_nested_metadata_properties(e) for e in metadata)
    if isinstance(metadata, set):
        return {clean_nested_metadata_properties(e) for e in metadata}
    return metadata


class WeaviatePersistor(WeaviateClientProcessor):

    def create_collection(
        self,
        persist_params: dict,
    ) -> Collection:
        """
        Create a new collection with the given name and the configuration that is compiled from
        the given persist_params. Raises a ValueError when a collection with that name
        already exists.

        :param persist_params: the configuration parameters to create the collection with
        :return: the newly created collection
        """
        collection_name, _ = self.compile_collection_tenant_names(
            persist_params.get("collection_name", None),
        )

        with self.client_factory as client:
            try:
                return client.collections.create(
                    name=collection_name,
                    properties=_compile_properties(persist_params),
                    multi_tenancy_config=_compile_multi_tenancy(persist_params),
                    references=_compile_cross_references(persist_params),
                )
            except UnexpectedStatusCodeError as e:
                logger.error(
                    "Failed to create collection '{collection_name}', error={error}",
                    collection_name=collection_name,
                    error=str(e),
                )
                raise ValueError("Failed to create collection") from e

    def create_tenant(
        self,
        collection: Collection,
        tenant_name: Optional[str],
    ) -> Collection:
        """
        Create a new tenant for a collection with a given name. If a tenant already exists,
        with the given tenant name, a ValueError is raised.

        :param collection: the Collection to create the tenant in
        :param tenant_name: the name of the tenant to add
        """
        tenant_name = tenant_name or self.base_params.tenant_name
        if tenant_name is None:
            logger.error("Cannot create tenant without a tenant name")
            raise ValueError("Cannot create tenant with no tenant name")

        collection.tenants.create([tenant_name])
        return collection.with_tenant(tenant_name)

    def persist_document(
        self,
        document: ChunkedDocument,
        collection_name: Optional[str] = None,
        tenant_name: Optional[str] = None,
        vector_name: str = "default",
    ) -> tuple[str, Optional[str], int, int]:
        """
        Persist a given chunked document into a collection with the given name and potentially
        into a tenant with the given name.

        The hierarchy of the chunks in this document is retained and the document filename and
        other metadata is persisted with each and every Object.

        :param document: the `ChunkedDocument` to persist
        :param collection_name: the name of the collection to store it into
        :param tenant_name: an Optional name of a tenant to store the document into.
        :param vector_name: the name of the vector to store the document embeddings into.
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

        if not collection.exists():
            logger.error(
                "collection '{collection_name}' does not exist.",
                collection_name=collection_name,
            )
            raise KeyError(f"Collection {collection_name} does not exist")

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
                    "document_metadata": clean_nested_metadata_properties(document.document_metadata),
                }
                references = {"parent": chunk.parent_id} if chunk.parent_id else None
                vector = {vector_name: chunk.embedding} if chunk.embedding else None

                if not collection.data.exists(chunk.chunk_id):
                    logger.debug(
                        "inserting chunk with id {chunk_id}", chunk_id=chunk.chunk_id
                    )
                    collection.data.insert(
                        uuid=chunk.chunk_id,
                        properties=properties,
                        references=references,
                        vector=vector,
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
                        vector=vector,
                    )
                    nr_replaced += 1

        return collection_name, tenant_name, nr_inserted, nr_replaced
