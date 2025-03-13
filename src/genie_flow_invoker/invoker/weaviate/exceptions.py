from typing import Optional


class NoMultiTenancySupportException(Exception):

    def __init__(self, collection_name: str, tenant_name: Optional[str], message: str):
        self.collection_name = collection_name
        self.tenant_name = tenant_name
        super(NoMultiTenancySupportException, self).__init__(message)


class TenantNotFoundException(Exception):
    def __init__(self, collection_name: str, tenant_name: str, message: str):
        self.collection_name = collection_name
        self.tenant_name = tenant_name
        super(TenantNotFoundException, self).__init__(message)


class CollectionNotFoundException(Exception):
    def __init__(self, collection_name: str, message: str):
        self.collection_name = collection_name
        super(CollectionNotFoundException, self).__init__(message)


class InvalidFilterException(Exception):
    def __init__(self, collection_name: str, tenant_name: Optional[str], message: str):
        self.collection_name = collection_name
        self.tenant_name = tenant_name
        super(InvalidFilterException, self).__init__(message)
