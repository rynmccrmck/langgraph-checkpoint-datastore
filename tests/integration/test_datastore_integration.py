import os
import uuid
import pytest
from google.cloud import datastore
from langgraph_checkpoint_datastore.saver import DatastoreSaver
from langgraph_checkpoint_datastore.write import Write

# A dummy serializer for integration testing.
class DummySerde:
    def dumps_typed(self, obj):
        return ("dummy", obj)
    
    def loads_typed(self, pair):
        return pair[1]

@pytest.fixture
def real_datastore_client():
    # Use the DATASTORE_PROJECT environment variable to run integration tests.
    project = os.environ.get("DATASTORE_PROJECT")
    if not project:
        pytest.skip("Skipping integration test because DATASTORE_PROJECT is not set")
    return datastore.Client(project=project)

@pytest.fixture
def datastore_saver_integration(real_datastore_client):
    # Create a DatastoreSaver instance that uses real_datastore_client.
    saver = DatastoreSaver(
        project=real_datastore_client.project,
        serde=DummySerde(),
        checkpoints_kind="IntegrationCheckpoint",
        writes_kind="IntegrationWrite"
    )
    # Override the client to use our real client.
    saver.client = real_datastore_client
    return saver

@pytest.fixture
def unique_config():
    # Generate unique identifiers so that tests do not conflict.
    thread_id = "test_thread_" + str(uuid.uuid4())
    checkpoint_id = "cp_" + str(uuid.uuid4())
    return {"configurable": {"thread_id": thread_id, "checkpoint_ns": "integration", "checkpoint_id": checkpoint_id}}

@pytest.mark.integration
def test_put_and_get_tuple(datastore_saver_integration, unique_config):
    checkpoint = {"id": unique_config["configurable"]["checkpoint_id"], "data": "checkpoint_integration"}
    metadata = {"info": "metadata_integration"}
    new_versions = {}
    # Store the checkpoint.
    config_out = datastore_saver_integration.put(unique_config, checkpoint, metadata, new_versions)
    # Retrieve it using get_tuple.
    tuple_result = datastore_saver_integration.get_tuple(config_out)
    assert tuple_result is not None
    assert tuple_result.checkpoint == checkpoint
    assert tuple_result.metadata == metadata

@pytest.mark.integration
def test_put_writes_and_get_tuple(datastore_saver_integration, unique_config):
    checkpoint = {"id": unique_config["configurable"]["checkpoint_id"], "data": "checkpoint_for_writes"}
    metadata = {"info": "metadata_for_writes"}
    new_versions = {}
    # Store the checkpoint.
    config_out = datastore_saver_integration.put(unique_config, checkpoint, metadata, new_versions)
    # Store two write items.
    writes = [("channel1", {"value": "write1"}), ("channel2", {"value": "write2"})]
    task_id = "integration_task"
    datastore_saver_integration.put_writes(config_out, writes, task_id)
    # Retrieve the checkpoint tuple and verify that pending writes are present.
    tuple_result = datastore_saver_integration.get_tuple(config_out)
    assert tuple_result is not None
    assert len(tuple_result.pending_writes) >= 2

@pytest.mark.integration
def test_list_checkpoints(datastore_saver_integration, unique_config):
    # Insert two checkpoints for the same thread.
    config1 = unique_config
    checkpoint1 = {"id": unique_config["configurable"]["checkpoint_id"], "data": "first_checkpoint"}
    metadata1 = {"info": "first_metadata"}
    new_versions = {}
    datastore_saver_integration.put(config1, checkpoint1, metadata1, new_versions)
    
    # Create a second checkpoint with a different checkpoint_id.
    checkpoint_id2 = "cp_" + str(uuid.uuid4())
    config2 = {
        "configurable": {
            "thread_id": unique_config["configurable"]["thread_id"],
            "checkpoint_ns": "integration",
            "checkpoint_id": checkpoint_id2,
        }
    }
    checkpoint2 = {"id": checkpoint_id2, "data": "second_checkpoint"}
    metadata2 = {"info": "second_metadata"}
    datastore_saver_integration.put(config2, checkpoint2, metadata2, new_versions)
    
    # List checkpoints for the thread.
    list_result = list(datastore_saver_integration.list({
        "configurable": {"thread_id": unique_config["configurable"]["thread_id"]}
    }))
    # Verify that at least 2 checkpoints exist.
    assert len(list_result) >= 2
