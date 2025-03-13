from typing import Any, NamedTuple, Optional, overload

from genie_flow_invoker.invoker.weaviate import WeaviateClientFactory
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
            raise ValueError("collection_name is required")
        return result

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
        if isinstance(collection_name_or_params, dict):
            collection_name = collection_name_or_params.get("collection_name", None)
            tenant_name = collection_name_or_params.get("tenant_name", None)
        else:
            collection_name = collection_name_or_params

        collection_name, tenant_name = self.compile_collection_tenant_names(
            collection_name,
            tenant_name,
        )
        with self.client_factory as client:
            collection = client.collections.get(collection_name)

        if tenant_name is None:
            return collection
        return collection.with_tenant(tenant_name)
