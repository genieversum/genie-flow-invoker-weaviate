from typing import Optional
import uuid

from genie_flow_invoker.doc_proc import ChunkedDocument
from loguru import logger

from weaviate.classes.query import Filter
from weaviate.collections.classes.batch import DeleteManyReturn

from .base import WeaviateClientProcessor
from .exceptions import (
    CollectionNotFoundException,
    InvalidFilterException,
    NoMultiTenancySupportException,
    TenantNotFoundException,
    NoCollectionProvided,
    NoTenantProvided,
)
from .utils import compile_filter


def _compile_results(result: DeleteManyReturn) -> dict[str, int]:
    return {k: getattr(result, k) for k in ["matches", "failed", "successful"]}


class WeaviateDeleter(WeaviateClientProcessor):

    def delete_chunks_by_id(
        self,
        chunk_ids: str | list[str] | uuid.UUID | list[uuid.UUID],
        collection_name: Optional[str] = None,
        tenant_name: Optional[str] = None,
    ) -> dict[str, int]:
        if not isinstance(chunk_ids, list):
            chunk_ids = [chunk_ids]
        chunk_ids = [uuid.UUID(s) if isinstance(s, str) else s for s in chunk_ids]
        collection = self.get_collection_or_tenant(collection_name, tenant_name)
        return _compile_results(
            collection.data.delete_many(
                where=Filter.by_id().contains_any(chunk_ids),
            )
        )

    def delete_chunks_by_filename(
        self,
        filename: str,
        collection_name: Optional[str] = None,
        tenant_name: Optional[str] = None,
    ) -> dict[str, int]:
        collection = self.get_collection_or_tenant(collection_name, tenant_name)
        return _compile_results(
            collection.data.delete_many(
                where=Filter.by_property("filename").equal(filename)
            )
        )

    def delete_chunked_document(
        self,
        chunked_document: ChunkedDocument,
        collection_name: Optional[str] = None,
        tenant_name: Optional[str] = None,
    ) -> dict[str, int]:
        collection = self.get_collection_or_tenant(collection_name, tenant_name)
        ids = [chunk.chunk_id for chunk in chunked_document.chunks]
        return _compile_results(
            collection.data.delete_many(where=Filter.by_id().contains_any(ids))
        )

    def delete_by_filter(
        self,
        by_filter: dict,
        collection_name: Optional[str] = None,
        tenant_name: Optional[str] = None,
    ) -> dict[str, int]:
        chunk_filter = compile_filter(by_filter)
        if chunk_filter is None:
            collection_name, tenant_name = self.compile_collection_tenant_names(
                collection_name,
                tenant_name,
            )
            raise InvalidFilterException(
                collection_name=collection_name,
                tenant_name=tenant_name,
                message="Invalid or empty filter",
            )

        collection = self.get_collection_or_tenant(collection_name, tenant_name)
        return _compile_results(collection.data.delete_many(where=chunk_filter))

    def delete_tenant(
        self,
        collection_name: Optional[str] = None,
        tenant_name: Optional[str] = None,
    ) -> dict[str, Optional[str]]:
        collection_name, tenant_name = self.compile_collection_tenant_names(
            collection_name,
            tenant_name,
        )

        if not tenant_name:
            logger.error("Trying to delete tenant but no tenant name provided")
            raise NoTenantProvided(
                collection_name=collection_name,
                tenant_name=tenant_name,
                message="Trying to delete tenant but no tenant name provided",
            )
        with self.client_factory as client:
            collection = client.collections.get(collection_name)

        config = collection.config.get()
        if not config.multi_tenancy_config.enabled:
            raise NoMultiTenancySupportException(
                collection_name=collection_name,
                tenant_name=tenant_name,
                message="This collection does not support multi-tenancy",
            )
        if not collection.tenants.exists(tenant_name):
            raise TenantNotFoundException(
                collection_name=collection_name,
                tenant_name=tenant_name,
                message="This tenant does not exist in this collection",
            )
        collection.tenants.remove([tenant_name])
        return {
            "collection_name": collection_name,
            "tenant_name": tenant_name,
        }

    def delete_collection(
        self,
        collection_name: Optional[str] = None,
    ) -> dict[str, str]:
        collection_name = collection_name or self.base_params.collection_name
        if collection_name is None:
            logger.error("Trying to delete collection but no collection name provided")
            raise ValueError(
                "Trying to delete collection but no collection name provided"
            )
        with self.client_factory as client:
            if not client.collections.exists(collection_name):
                raise CollectionNotFoundException(
                    collection_name=collection_name,
                    tenant_name=None,
                    message=f"This collection does not exist",
                )
            client.collections.delete(collection_name)

        return {"collection_name": collection_name}
