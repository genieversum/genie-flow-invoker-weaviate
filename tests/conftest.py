import uuid
from collections import defaultdict, namedtuple
from typing import Optional

from weaviate.collections.classes.internal import Object
from pytest import fixture


class MockQuery:

    def __init__(self, query_results: list[Object]):
        self.query_results = query_results

    def near_text(self, **kwargs):
        return self.query_results

    def near_vector(self, **kwargs):
        return self.query_results

    def hybrid(self, **kwargs):
        return self.query_results


class MockCollection:

    def __init__(self, collection_name: str, query_results: Optional[list[Object]] = None):
        self.collection_name = collection_name
        self.query_results = query_results if query_results else []

    @property
    def query(self):
        return MockQuery(self.query_results)

    @property
    def tenants(self):
        tenant_name = f"Tenant{self.collection_name}"
        return MockCollections({tenant_name:self.query_results})

    @property
    def aggregate(self):
        return MockAggregate(self.query_results)


class MockAggregate:

    def __init__(self, query_results: list[Object]):
        self.query_results = query_results

    def over_all(self, **kwargs):
        return self

    @property
    def properties(self):
        return self

    def __getitem__(self, item):
        return self

    @property
    def maximum(self):
        return max(x.properties["hierarchy_level"] for x in self.query_results)



class MockCollections:

    def __init__(self, collections_results: Optional[dict] = None):
        if collections_results is None:
            collections_results = dict()
        self.collections = {
            collection_name: MockCollection(collection_name, collection_results)
            for collection_name, collection_results in collections_results.items()
        }

    def get(self, collection_name: str):
        return self.collections[collection_name]

class MockWeaviateClient:

    def __init__(self, collections_results: dict):
        self.collections = MockCollections(collections_results)


class MockWeaviateClientFactory:

    def __init__(self, collections_results: dict):
        self.collections_results = collections_results

    def __enter__(self):
        return MockWeaviateClient(self.collections_results)

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


@fixture
def collections_results():
    return dict(
        SimpleCollection=[
            Object(
                uuid=uuid.uuid3(uuid.NAMESPACE_OID, "first document"),
                properties=dict(
                    filename="some_file.txt",
                    content="Hello World",
                    original_span_start=0,
                    original_span_end=42,
                    hierarchy_level=0,
                    document_metadata=dict(
                        language="en",
                        source="pdf",
                    ),
                ),
                metadata=dict(),
                references=dict(
                    parent=[
                        Object(
                            collection="BIMBAM",
                            uuid=uuid.uuid3(uuid.NAMESPACE_OID, "second document"),
                            properties=dict(
                                filename="some_file.txt",
                                content="Hello Parent",
                                original_span_start=0,
                                original_span_end=42,
                                hierarchy_level=0,
                                document_metadata=dict(
                                    language="en",
                                    source="pdf",
                                ),
                            ),
                            metadata=dict(),
                            references=None,
                            vector=dict(default=[2.27]*12, low_space=[2.3]*3),
                        )
                    ]
                ),
                collection="SimpleCollection",
                vector=dict(default=[3.14]*12, low_space=[3.1]*3),
            )
        ]
    )


@fixture
def weaviate_client_factory(collections_results):
    return MockWeaviateClientFactory(collections_results)
