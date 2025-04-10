from typing import Optional


class WeaviateDeleteException(Exception):
    def __init__(self, collection_name: str, tenant_name: Optional[str], message: str):
        self.collection_name = collection_name
        self.tenant_name = tenant_name
        super(WeaviateDeleteException, self).__init__(message)


class NoCollectionProvided(WeaviateDeleteException): ...


class NoTenantProvided(WeaviateDeleteException): ...


class NoMultiTenancySupportException(WeaviateDeleteException): ...


class TenantNotFoundException(WeaviateDeleteException): ...


class CollectionNotFoundException(WeaviateDeleteException): ...


class InvalidFilterException(WeaviateDeleteException): ...
