from typing import Any, NamedTuple, Optional, overload

from genie_flow_invoker.invoker.weaviate import WeaviateClientFactory
from genie_flow_invoker.invoker.weaviate.exceptions import NoCollectionProvided
from loguru import logger
from weaviate.collections import Collection


class CollectionTenant(NamedTuple):
    collection_name: Optional[str]
    tenant_name: Optional[str]


class WeaviateClientProcessor:

    def __init__(
        self,
        client_factory: WeaviateClientFactory,
        processor_params: dict[str, Any],
    ):
        self.client_factory = client_factory
        self.base_params = CollectionTenant(
            collection_name=processor_params.get("collection_name", None),
            tenant_name=processor_params.get("tenant_name", None),
        )

    def compile_collection_tenant_names(
        self,
        collection_name: Optional[str] = None,
        tenant_name: Optional[str] = None,
    ) -> tuple[str, Optional[str]]:
        result = (
            collection_name or self.base_params.collection_name,
            tenant_name or self.base_params.tenant_name,
        )
        if result[0] is None:
            raise NoCollectionProvided(
                collection_name=result[0],
                tenant_name=result[1],
                message="collection_name is required",
            )
        return result

    @overload
    def get_collection(self, collection_name: str) -> Collection: ...

    @overload
    def get_collection(self, params: dict[str, Any]) -> Collection: ...

    def get_collection(self, collection_name_or_params: Optional[str | dict[str, Any]]) -> Collection:
        if isinstance(collection_name_or_params, dict):
            collection_name = collection_name_or_params.get("collection_name", None)
        else:
            collection_name = collection_name_or_params

        collection_name = collection_name or self.base_params.collection_name
        if collection_name is None:
            raise NoCollectionProvided(
                message="Collection name is required",
                collection_name="",
                tenant_name=None,
            )

        with self.client_factory as client:
            if not client.collections.exists(collection_name):
                raise KeyError(f"Collection {collection_name} does not exist")
            return client.collections.get(collection_name)


    @overload
    def get_collection_or_tenant(
        self,
        params: dict[str, Any],
    ) -> Collection: ...

    @overload
    def get_collection_or_tenant(
        self,
        collection_name: Optional[str],
        tenant_name: Optional[str],
    ) -> Collection: ...

    def get_collection_or_tenant(
        self,
        collection_name_or_params: Optional[str | dict[str, Any]] = None,
        tenant_name: Optional[str] = None,
    ) -> Collection:
        collection = self.get_collection(collection_name_or_params)

        if isinstance(collection_name_or_params, dict):
            tenant_name = collection_name_or_params.get("tenant_name", None)

        tenant_name = tenant_name or self.base_params.tenant_name
        if tenant_name is None:
            return collection

        if not collection.tenants.exists(tenant_name):
            logger.error(
                "Tenant {tenant_name} does not exist in collection {collection_name}",
                tenant_name=tenant_name,
                collection_name=collection.name,
            )
            raise KeyError(f"Tenant {tenant_name} does not exist in collection {collection.name}")

        return collection.with_tenant(tenant_name)
