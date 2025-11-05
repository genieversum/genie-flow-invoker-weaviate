import re
from collections import defaultdict
from typing import Optional, Any

from genie_flow_invoker.doc_proc import ChunkedDocument, DocumentChunk
from loguru import logger
from weaviate.exceptions import UnexpectedStatusCodeError

from .base import WeaviateClientProcessor
from weaviate.classes.config import (
    Configure,
    DataType,
    Property,
    ReferenceProperty,
)
from weaviate.collections.classes.data import DataObject
from weaviate.collections import Collection

from .properties import flatten_properties


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
            "vectorizer": "none",
        }
    }
    config.update(params.get("named_vectors", {}))
    return params.get(
        "named_vectors",
        [
            getattr(Configure.NamedVectors, value["vectorizer"])(
                name=key,
                vector_index_config=Configure.VectorIndex.flat(),
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


def _build_properties(document: ChunkedDocument, chunk: DocumentChunk) -> dict[str, Any]:

    props: dict[str, Any] = {
        "filename": document.filename,
        "content": chunk.content,
        "original_span_start": chunk.original_span[0],
        "original_span_end": chunk.original_span[1],
        "hierarchy_level": chunk.hierarchy_level,
    }
    properties_to_flatten = {
        "document_metadata": document.document_metadata,
        "custom_properties": chunk.custom_properties,
    }
    flats = flatten_properties(properties_to_flatten)
    for prop in flats:
        props[prop.flat_name] = prop.value
    props["property_map"] = {prop.flat_name: prop.path for prop in flats}
    return props


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
        params = {
            "collection_name": self.base_params.collection_name,
            "tenant_name": self.base_params.tenant_name,
        }
        params.update(persist_params)

        collection_name, _ = self.compile_collection_tenant_names(
            params.get("collection_name", None),
        )

        with self.client_factory as client:
            try:
                return client.collections.create(
                    name=collection_name,
                    properties=_compile_properties(params.get("properties", {})),
                    multi_tenancy_config=_compile_multi_tenancy(params),
                    references=_compile_cross_references(params),
                    vectorizer_config=[
                        Configure.NamedVectors.none(
                            name="default",
                            vector_index_config=Configure.VectorIndex.flat()
                        )],
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
        document: ChunkedDocument | list[ChunkedDocument],
        collection_name: Optional[str] = None,
        tenant_name: Optional[str] = None,
        vector_name: str = "default",
        batch_size: int = 1000,
    ) -> tuple[str, Optional[str], int, int]:
        """
        Persist a given chunked document or a list of chunked documents into a collection with the 
        given name and potentially into a tenant with the given name.

        The hierarchy of the chunks in this document is retained and the document filename and
        other metadata is persisted with each and every Object.

        :param document: the `ChunkedDocument` to persist
        :param collection_name: the name of the collection to store it into
        :param tenant_name: an Optional name of a tenant to store the document into.
        :param vector_name: the name of the vector to store the document embeddings into.
        :param batch_size: the number of chunks to insert in a single batch
        :return: tuple of the used collection_name and tenant_name, nr_inserted and nr_replaces,
                respectively the number of inserted and replaced chunks
        """
        collection_name, tenant_name = self.compile_collection_tenant_names(
            collection_name, tenant_name
        )

        documents = [document] if not isinstance(document, list) else document
        del document  # unbind this variable to avoid confusion with loop variable

        chunk_index: dict[int, list[tuple[DocumentChunk, ChunkedDocument]]] = defaultdict(list)
        nr_chunks = 0
        nr_files = 0
        for document in documents:
            nr_files += 1
            for chunk in document.chunks:
                nr_chunks += 1
                chunk_index[chunk.hierarchy_level].append((chunk, document))

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
            "for '{nr_files}' files",
            collection_name=collection.name,
            nr_chunks=nr_chunks,
            nr_files=nr_files,
        )

        # making sure we add the chunks from top to bottom
        nr_inserted = 0
        nr_replaced = 0
        hierarchy_levels = sorted(chunk_index.keys())
        for hierarchy_level in hierarchy_levels:
            chunks = chunk_index[hierarchy_level]
            logger.debug(
                "persisting {nr_chunks} chunk(s) at hierarchy level {hierarchy_level}",
                nr_chunks=len(chunks),
                hierarchy_level=hierarchy_level,
            )
            chunk_buffer = []
            for chunk, document in chunks:
                properties = _build_properties(document, chunk)
                references = {"parent": chunk.parent_id} if chunk.parent_id else None
                vector = {vector_name: chunk.embedding} if chunk.embedding else None

                if not collection.data.exists(chunk.chunk_id):
                    logger.debug("adding chunk with id {chunk_id} to buffer", chunk_id=chunk.chunk_id)
                    chunk_buffer.append(
                        DataObject(
                            uuid=chunk.chunk_id,
                            properties=properties,
                            references=references,
                            vector=vector,
                        )
                    )
                else:
                    logger.debug("replacing chunk with id {chunk_id}", chunk_id=chunk.chunk_id)
                    # TODO: use batch replace when available - weaviate currently does not support batch replace
                    collection.data.replace(
                        uuid=chunk.chunk_id,  # type: ignore
                        properties=properties,
                        references=references, # potential issue: parent not existing in weaviate
                        vector=vector,
                    )
                    nr_replaced += 1

                while len(chunk_buffer) >= batch_size:
                    nr_inserted += drain_buffer(collection, chunk_buffer)
                    
            # drain remaining buffer
            nr_inserted += drain_buffer(collection, chunk_buffer)
        return collection_name, tenant_name, nr_inserted, nr_replaced


def drain_buffer(collection: Collection, chunk_buffer: list[DataObject]) -> int:
    if chunk_buffer:
        logger.debug("inserting batch of {batch_size} chunks", batch_size=len(chunk_buffer))
        collection.data.insert_many(chunk_buffer)
        nr_inserted = len(chunk_buffer)
        chunk_buffer.clear()
        return nr_inserted
    return 0