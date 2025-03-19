import uuid
from typing import Optional, Any

from weaviate.collections.classes.internal import Object
from pytest import fixture

from genie_flow_invoker.doc_proc import ChunkedDocument, DocumentChunk


class MockQuery:

    def __init__(self, query_results: list[Object]):
        self.query_results = query_results

    def near_text(self, **kwargs):
        return self.query_results

    def near_vector(self, **kwargs):
        return self.query_results

    def hybrid(self, **kwargs):
        return self.query_results


class MockCollectionData:

    def __init__(self, collection_name: str, query_results: list[Object]):
        self.collection_name = collection_name
        self.query_results = query_results

    def insert(
            self,
            uuid: uuid.UUID,
            properties: dict[str, Any],
            references: dict[str, Any],
            vector: list[float],
    ):
        self.query_results.append(
            Object(
                collection=self.collection_name,
                uuid=uuid,
                properties=properties,
                references=references,
                vector={
                    "default": vector,
                },
                metadata={},
            )
        )

    def replace(
            self,
            uuid: uuid.UUID,
            properties: dict[str, Any],
            references: dict[str, Any],
            vector: list[float],
    ):
        self.query_results = [d for d in self.query_results if d.uuid != uuid]
        self.insert(uuid, properties, references, vector)

    def exists(self, uuid: uuid.UUID) -> bool:
        for mine in self.query_results:
            if str(mine.uuid) == uuid:
                return True
        return False


class MockCollection:

    def __init__(self, collection_name: str, query_results: Optional[list[Object]] = None):
        self.name = collection_name
        self.query_results = query_results if query_results else []

    @property
    def query(self):
        return MockQuery(self.query_results)

    @property
    def tenants(self):
        tenant_name = f"Tenant{self.name}"
        return MockCollections({tenant_name:self.query_results})

    @property
    def data(self):
        return MockCollectionData(self.name, self.query_results)

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
                collection="SimpleCollection",
                uuid=uuid.uuid3(uuid.NAMESPACE_OID, "first document"),
                properties=dict(
                    filename="some_file.txt",
                    content="Hello World",
                    original_span_start=0,
                    original_span_end=42,
                    hierarchy_level=1,
                    document_metadata=dict(
                        language="en",
                        source="pdf",
                    ),
                ),
                metadata=dict(),
                references=dict(
                    parent=[
                        Object(
                            collection="SimpleCollection",
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
                vector=dict(default=[3.14]*12, low_space=[3.1]*3),
            )
        ]
    )


@fixture
def weaviate_client_factory(collections_results):
    return MockWeaviateClientFactory(collections_results)


@fixture
def chunked_document():
    return ChunkedDocument(
        filename="some_file.txt",
        document_metadata={
            "language": "en",
            "source": "pdf",
        },
        chunks=[
            DocumentChunk(
                chunk_id=str(uuid.uuid3(uuid.NAMESPACE_OID, "second document")),
                content="Hello Parent",
                original_span=(0, 42),
                hierarchy_level=0,
                parent_id=None,
                embedding=[2.27]*12,
            ),
            DocumentChunk(
                chunk_id=str(uuid.uuid3(uuid.NAMESPACE_OID, "first document")),
                content="Hello World",
                original_span=(0, 42),
                hierarchy_level=1,
                parent_id=str(uuid.uuid3(uuid.NAMESPACE_OID, "second document")),
                embedding=[3.14]*12,
            )
        ]
    )
