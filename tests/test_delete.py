import uuid
from collections import namedtuple

from conftest import (
    Recorder,
    MockCollectionData,
    MockConfig,
    MockCollection,
    MockCollections,
)
from genie_flow_invoker.invoker.weaviate import (
    WeaviateDeleter,
    InvalidFilterException,
    NoMultiTenancySupportException,
    TenantNotFoundException,
    CollectionNotFoundException,
)
import pytest
from weaviate.collections.classes.batch import DeleteManyReturn
from weaviate.collections.classes.filters import (
    Filter,
    _FilterAnd,
    _Operator,
    _FilterOr,
)

from genie_flow_invoker.invoker.weaviate.exceptions import (
    NoTenantProvided,
    NoCollectionProvided,
)
from genie_flow_invoker.invoker.weaviate.properties import create_flat_name


def test_delete_by_id(weaviate_client_factory, monkeypatch):
    delete_return_value = DeleteManyReturn(
        matches=128,
        objects=[],
        failed=64,
        successful=64,
    )
    recorder = Recorder(delete_return_value)
    monkeypatch.setattr(
        MockCollectionData,
        "delete_many",
        recorder.record,
    )

    deleter = WeaviateDeleter(
        weaviate_client_factory,
        {"collection_name": "SimpleCollection"},
    )

    chunk_id = uuid.uuid3(uuid.NAMESPACE_OID, "first document")
    deleter.delete_chunks_by_id(str(chunk_id))

    assert len(recorder.recording) == 1
    assert len(recorder.recording[0][0]) == 0
    filter_kwargs = recorder.recording[0][1]
    assert "where" in filter_kwargs.keys()
    assert filter_kwargs["where"].operator == _Operator.CONTAINS_ANY
    assert filter_kwargs["where"].value == [str(chunk_id)]
    assert filter_kwargs["where"].target == "_id"


def test_delete_by_filename(weaviate_client_factory, monkeypatch):
    delete_return_value = DeleteManyReturn(
        matches=128,
        objects=[],
        failed=64,
        successful=64,
    )
    recorder = Recorder(delete_return_value)
    monkeypatch.setattr(
        MockCollectionData,
        "delete_many",
        recorder.record,
    )

    deleter = WeaviateDeleter(
        weaviate_client_factory,
        {"collection_name": "SimpleCollection"},
    )

    deleter.delete_chunks_by_filename("some-filename.txt")

    assert len(recorder.recording) == 1
    assert len(recorder.recording[0][0]) == 0
    filter_kwargs = recorder.recording[0][1]
    assert "where" in filter_kwargs.keys()
    assert filter_kwargs["where"].operator == _Operator.EQUAL
    assert filter_kwargs["where"].value == "some-filename.txt"
    assert filter_kwargs["where"].target == "filename"


def test_delete_by_filter(weaviate_client_factory, monkeypatch):
    delete_return_value = DeleteManyReturn(
        matches=128,
        objects=[],
        failed=64,
        successful=64,
    )
    recorder = Recorder(delete_return_value)
    monkeypatch.setattr(
        MockCollectionData,
        "delete_many",
        recorder.record,
    )

    filter_specs = {
        "having_all": {
            "first_all": 123,
            "second_all !=": 0,
        }
    }
    deleter = WeaviateDeleter(
        weaviate_client_factory,
        {"collection_name": "SimpleCollection"},
    )

    deleter.delete_by_filter(filter_specs)

    assert len(recorder.recording) == 1
    assert len(recorder.recording[0][0]) == 0
    filter_kwargs = recorder.recording[0][1]
    assert "where" in filter_kwargs.keys()
    all_filter = recorder.recording[0][1]["where"]
    assert len(all_filter.filters) == 2
    assert all_filter.filters[0].operator == _Operator.EQUAL
    assert all_filter.filters[1].target == create_flat_name("second_all")


def test_delete_failed_filter(weaviate_client_factory):
    deleter = WeaviateDeleter(
        weaviate_client_factory,
        {"collection_name": "SimpleCollection"},
    )

    with pytest.raises(InvalidFilterException) as e:
        deleter.delete_by_filter({})
        assert e.collection_name == "SimpleCollection"
        assert e.tenant_name is None
        assert e.message == "Invalid or empty filter"


def test_delete_result(weaviate_client_factory, monkeypatch):
    delete_return_value = DeleteManyReturn(
        matches=128,
        objects=["some", "objects"],
        failed=1,
        successful=127,
    )
    recorder = Recorder(delete_return_value)
    monkeypatch.setattr(
        MockCollectionData,
        "delete_many",
        recorder.record,
    )
    deleter = WeaviateDeleter(
        weaviate_client_factory,
        {"collection_name": "SimpleCollection"},
    )
    deleter_result = deleter.delete_chunks_by_id(uuid.uuid4())
    assert deleter_result["matches"] == 128
    assert deleter_result["failed"] == 1
    assert deleter_result["successful"] == 127
    assert len(deleter_result.keys()) == 3


def test_delete_tenant_none(weaviate_client_factory, monkeypatch):
    deleter = WeaviateDeleter(
        weaviate_client_factory,
        {},
    )
    with pytest.raises(NoTenantProvided) as e:
        deleter.delete_tenant("SimpleCollection", None)
        assert e.message == "Trying to delete tenant but no tenant name provided"

    with pytest.raises(NoTenantProvided) as e:
        deleter.delete_tenant("SimpleCollection", "")

    with pytest.raises(NoCollectionProvided) as e:
        deleter.delete_tenant(None, None)
        assert e.message == "Trying to delete tenant but no collection name provided"


def test_delete_tenant_not_multitenant(weaviate_client_factory, monkeypatch):
    monkeypatch.setattr(
        MockConfig,
        "multi_tenancy_config",
        namedtuple("MultiTenantConfig", ["enabled"], defaults=(False,))(),
    )
    deleter = WeaviateDeleter(weaviate_client_factory, {})

    with pytest.raises(NoMultiTenancySupportException) as e:
        deleter.delete_tenant("SimpleCollection", "SomeTenant")

        assert e.collection_name == "SimpleCollection"
        assert e.tenante_name == "SomeTenant"
        assert e.message == "This collection does not support multi-tenancy"


def test_delete_tenant_not_existing(weaviate_client_factory, monkeypatch):
    monkeypatch.setattr(
        MockConfig,
        "multi_tenancy_config",
        namedtuple("MultiTenantConfig", ["enabled"], defaults=(True,))(),
    )
    monkeypatch.setattr(
        MockCollections,
        "exists",
        lambda _, x: False,
    )
    deleter = WeaviateDeleter(weaviate_client_factory, {})
    with pytest.raises(TenantNotFoundException) as e:
        deleter.delete_tenant("SimpleCollection", "NonExistentTenant")
        e.collection_name == "SimpleCollection"
        e.tenant_name == "NonExistentTenant"
        e.message == "This tenant does not exist in this collection"


def test_delete_tenant(weaviate_client_factory, monkeypatch):
    monkeypatch.setattr(
        MockConfig,
        "multi_tenancy_config",
        namedtuple("MultiTenantConfig", ["enabled"], defaults=(True,))(),
    )
    monkeypatch.setattr(
        MockCollections,
        "exists",
        lambda _, x: True,
    )
    recorder = Recorder(
        {
            "collection_name": "SimpleCollection",
            "tenant_name": "SimpleTenant",
        }
    )
    monkeypatch.setattr(
        MockCollections,
        "remove",
        recorder.record,
    )

    deleter = WeaviateDeleter(
        weaviate_client_factory,
        {
            "collection_name": "SimpleCollection",
            "tenant_name": "SimpleTenant",
        },
    )
    delete_result = deleter.delete_tenant(
        "SimpleCollection",
        "SimpleTenant",
    )

    assert len(recorder.recording) == 1
    assert recorder.recording[0][0] == (["SimpleTenant"],)
    assert delete_result["collection_name"] == "SimpleCollection"
    assert delete_result["tenant_name"] == "SimpleTenant"


def test_delete_collection_no_name(weaviate_client_factory):
    deleter = WeaviateDeleter(weaviate_client_factory, {})
    with pytest.raises(ValueError) as e:
        deleter.delete_collection("")
        e.message == "Trying to delete collection but no collection name provided"


def test_delete_collection_non_existent(weaviate_client_factory, monkeypatch):
    monkeypatch.setattr(
        MockCollections,
        "exists",
        lambda _, x: False,
    )
    deleter = WeaviateDeleter(weaviate_client_factory, {})
    with pytest.raises(CollectionNotFoundException) as e:
        deleter.delete_collection("SomeCollection")
        assert e.collection_name == "SomeCollection"
        assert e.message == "This collection does not exist"


def test_delete_collection(weaviate_client_factory, monkeypatch):
    monkeypatch.setattr(
        MockCollections,
        "exists",
        lambda _, x: True,
    )
    deleter = WeaviateDeleter(
        weaviate_client_factory,
        {
            "collection_name": "SimpleCollection",
        },
    )
    recorder = Recorder(None)
    delete_result = deleter.delete_collection()
    assert delete_result["collection_name"] == "SimpleCollection"
