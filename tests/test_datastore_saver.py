import pytest
from unittest.mock import MagicMock
from langgraph_checkpoint_datastore.saver import DatastoreSaver
from langgraph_checkpoint_datastore.write import Write
from langgraph.checkpoint.base import CheckpointTuple

# A dummy serializer that simply returns the object unchanged.
class DummySerde:
    def dumps_typed(self, obj):
        # For our tests we assume the type is always "dummy"
        return ("dummy", obj)
    
    def loads_typed(self, pair):
        # Simply return the object (i.e. pair[1])
        return pair[1]

@pytest.fixture
def dummy_serde():
    return DummySerde()

@pytest.fixture
def fake_client():
    client = MagicMock()
    
    def fake_key(kind, key_name):
        key = MagicMock()
        key.name = key_name
        return key
    
    client.key.side_effect = fake_key
    return client

@pytest.fixture
def datastore_saver(dummy_serde, fake_client, monkeypatch):
    # Monkeypatch the Datastore client so that the saver uses our fake_client.
    def fake_client_constructor(project=None, **kwargs):
        return fake_client
    monkeypatch.setattr("langgraph_checkpoint_datastore.saver.datastore.Client", fake_client_constructor)
    saver = DatastoreSaver(
        project="fake-project",
        serde=dummy_serde,
        checkpoints_kind="Checkpoint",
        writes_kind="Write"
    )
    return saver

def test_get_tuple(datastore_saver, fake_client):
    # Prepare a fake checkpoint entity as would be stored in Datastore.
    fake_entity = {
        "thread_id": "t1",
        "checkpoint_ns": "ns",
        "checkpoint_id": "cp1",
        "parent_checkpoint_id": "cp0",
        "type": "dummy",
        "checkpoint": {"id": "cp1", "data": "checkpoint_data"},
        "metadata": {"info": "metadata_data"}
    }
    # Configure the fake client's get() to return the fake entity.
    fake_client.get.return_value = fake_entity

    # Also set up a fake query for writes.
    fake_query = MagicMock()
    fake_write_entity = {
        "partition_key": Write.separator.join(["t1", "cp1", "ns"]),
        "sort_key": Write.separator.join(["task1", "0"]),
        "channel": "ch1",
        "type": "dummy",
        "value": "write_value"
    }
    fake_query.fetch.return_value = [fake_write_entity]
    fake_client.query.return_value = fake_query

    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": "ns", "checkpoint_id": "cp1"}}
    tuple_result = datastore_saver.get_tuple(config)
    assert tuple_result is not None
    assert tuple_result.config["configurable"]["thread_id"] == "t1"
    assert tuple_result.checkpoint == fake_entity["checkpoint"]
    assert tuple_result.metadata == fake_entity["metadata"]
    # Check that the pending writes list contains one write.
    assert len(tuple_result.pending_writes) == 1
    task_id, channel, value = tuple_result.pending_writes[0]
    assert task_id == "task1"
    assert channel == "ch1"
    assert value == "write_value"

def test_put(datastore_saver, fake_client):
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": "ns", "checkpoint_id": "old_cp"}}
    checkpoint = {"id": "new_cp", "data": "checkpoint_data"}
    metadata = {"info": "metadata_data"}
    new_versions = {}  # Not used by this implementation

    # Call the put() method.
    new_config = datastore_saver.put(config, checkpoint, metadata, new_versions)
    # Verify that the fake client's put() was called exactly once.
    fake_client.put.assert_called_once()
    # Retrieve the entity that was put.
    put_entity = fake_client.put.call_args[0][0]
    # Check that the key name is built correctly.
    expected_key_name = f"t1:::" + "ns:::" + "new_cp"
    # In our Datastore client, the key is an object; we check its .name attribute.
    assert put_entity.key.name == expected_key_name
    # Verify that the entity contains the expected attributes.
    assert put_entity["thread_id"] == "t1"
    assert put_entity["checkpoint_ns"] == "ns"
    assert put_entity["checkpoint_id"] == "new_cp"
    assert put_entity["parent_checkpoint_id"] == "old_cp"
    assert put_entity["type"] == "dummy"
    assert put_entity["checkpoint"] == checkpoint
    assert put_entity["metadata"] == metadata

def test_put_writes(datastore_saver, fake_client):
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": "ns", "checkpoint_id": "cp1"}}
    writes = [("ch1", {"data": "write1"}), ("ch2", {"data": "write2"})]
    task_id = "task123"
    datastore_saver.put_writes(config, writes, task_id)
    # Verify that the fake client's put_multi() was called with two entities.
    fake_client.put_multi.assert_called_once()
    put_entities = fake_client.put_multi.call_args[0][0]
    assert len(put_entities) == 2
    # For each entity, verify that the partition_key and sort_key are built correctly.
    for idx, entity in enumerate(put_entities):
        expected_partition = Write.separator.join(["t1", "cp1", "ns"])
        expected_sort = Write.separator.join([task_id, str(idx)])
        assert entity["partition_key"] == expected_partition
        assert entity["sort_key"] == expected_sort
        # Verify additional fields.
        if idx == 0:
            assert entity["channel"] == "ch1"
            assert entity["type"] == "dummy"
            assert entity["value"] == {"data": "write1"}
        else:
            assert entity["channel"] == "ch2"
            assert entity["type"] == "dummy"
            assert entity["value"] == {"data": "write2"}

def test_list(datastore_saver, fake_client):
    # Create two fake checkpoint entities.
    fake_entity1 = {
        "thread_id": "t1",
        "checkpoint_ns": "ns",
        "checkpoint_id": "cp2",
        "type": "dummy",
        "checkpoint": {"id": "cp2", "data": "checkpoint2"},
        "metadata": {"info": "metadata2"}
    }
    fake_entity2 = {
        "thread_id": "t1",
        "checkpoint_ns": "ns",
        "checkpoint_id": "cp1",
        "type": "dummy",
        "checkpoint": {"id": "cp1", "data": "checkpoint1"},
        "metadata": {"info": "metadata1"}
    }
    # Set up a fake query that returns these entities.
    fake_query = MagicMock()
    fake_query.fetch.return_value = [fake_entity1, fake_entity2]
    fake_client.query.return_value = fake_query

    config = {"configurable": {"thread_id": "t1"}}
    tuples = list(datastore_saver.list(config))
    assert len(tuples) == 2
    # Check that the checkpoint IDs appear in the order provided by our fake query.
    assert tuples[0].config["configurable"]["checkpoint_id"] == "cp2"
    assert tuples[1].config["configurable"]["checkpoint_id"] == "cp1"
