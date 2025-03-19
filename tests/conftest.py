import uuid as uuidlib
from typing import Optional, Any

from weaviate.collections.classes.internal import Object
from weaviate.collections.classes.batch import DeleteManyReturn
from pytest import fixture

from genie_flow_invoker.doc_proc import ChunkedDocument, DocumentChunk


class Recorder:

    def __init__(self, return_value: Any):
        self.recording = []
        self._return_value = return_value

    def record(self, *args, **kwargs):
        self.recording.append((args, dict(**kwargs)))
        return self._return_value


class MockConfig:

    def get(self):
        return self

    @property
    def multi_tenancy_config(self):
        return True


class MockQuery:

    def __init__(self, query_results: list[Object]):
        self.query_results = query_results

    def fetch_objects(self):
        return self.query_results

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
        uuid: str | uuidlib.UUID,
        properties: dict[str, Any],
        references: dict[str, Any],
        vector: list[float],
    ):
        if isinstance(uuid, str):
            uuid = uuidlib.UUID(uuid)
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
        uuid: str | uuidlib.UUID,
        properties: dict[str, Any],
        references: dict[str, Any],
        vector: list[float],
    ):
        if type(uuid) is str:
            uuid = uuidlib.UUID(uuid)
        self.query_results = [d for d in self.query_results if d.uuid != uuid]
        self.insert(uuid, properties, references, vector)

    def exists(self, uuid: str | uuidlib.UUID) -> bool:
        if type(uuid) is str:
            uuid = uuidlib.UUID(uuid)
        for mine in self.query_results:
            if mine.uuid == uuid:
                return True
        return False

    def delete_many(self, **kwargs):
        return DeleteManyReturn(
            matches=len(self.query_results),
            objects=self.query_results,
            failed=0,
            successful=len(self.query_results),
        )


class MockCollection:

    def __init__(
        self, collection_name: str, query_results: Optional[list[Object]] = None
    ):
        self.name = collection_name
        self.query_results = query_results if query_results else []

    @property
    def query(self):
        return MockQuery(self.query_results)

    @property
    def tenants(self):
        tenant_name = f"Tenant{self.name}"
        return MockCollections({tenant_name: self.query_results})

    @property
    def data(self):
        return MockCollectionData(self.name, self.query_results)

    @property
    def aggregate(self):
        return MockAggregate(self.query_results)

    @property
    def config(self):
        return MockConfig()

    def with_tenant(self, tenant_name):
        return MockCollection(f"{self.name} / {tenant_name}", self.query_results)


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

    def exists(self, some_name: str):
        return True

    def remove(self, _: list[str]): ...

    def delete(self, _: str): ...


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
    parent = Object(
        collection="SimpleCollection",
        uuid=uuidlib.uuid3(uuidlib.NAMESPACE_OID, "second document"),
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
        vector=dict(default=[2.27] * 12, low_space=[2.3] * 3),
    )
    child = Object(
        collection="SimpleCollection",
        uuid=uuidlib.uuid3(uuidlib.NAMESPACE_OID, "first document"),
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
            parent=[parent],
        ),
        vector=dict(default=[3.14] * 12, low_space=[3.1] * 3),
    )
    return dict(
        SimpleCollection=[parent, child],
    )


@fixture
def weaviate_client_factory(collections_results):
    return MockWeaviateClientFactory(collections_results)


@fixture
def chunked_document():
    parent = DocumentChunk(
        chunk_id=str(uuidlib.uuid3(uuidlib.NAMESPACE_OID, "second document")),
        content="Hello Parent",
        original_span=(0, 42),
        hierarchy_level=0,
        parent_id=None,
        embedding=[2.27] * 12,
    )
    child = DocumentChunk(
        chunk_id=str(uuidlib.uuid3(uuidlib.NAMESPACE_OID, "first document")),
        content="Hello World",
        original_span=(0, 42),
        hierarchy_level=1,
        parent_id=parent.chunk_id,
        embedding=[3.14] * 12,
    )
    return ChunkedDocument(
        filename="some_file.txt",
        document_metadata={
            "language": "en",
            "source": "pdf",
        },
        chunks=[parent, child],
    )


@fixture
def other_chunked_document():
    one = DocumentChunk(
        chunk_id=str(uuidlib.uuid3(uuidlib.NAMESPACE_OID, "document one")),
        content="to Be or Not To Be, That is the Question",
        original_span=(255, 512),
        hierarchy_level=0,
        parent_id=None,
    )
    two = DocumentChunk(
        chunk_id=str(uuidlib.uuid3(uuidlib.NAMESPACE_OID, "document two")),
        content="Is this a dagger I see before me?",
        original_span=(1024, 4096),
        hierarchy_level=0,
        parent_id=None,
    )
    return ChunkedDocument(
        filename="shakespear.txt",
        document_metadata={
            "language": "en",
            "source": "txt",
        },
        chunks=[one, two],
    )
