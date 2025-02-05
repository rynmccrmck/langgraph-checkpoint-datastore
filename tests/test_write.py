import pytest
from langgraph_checkpoint_datastore.write import Write

def test_to_dict_and_keys():
    write = Write(
        thread_id="t1",
        checkpoint_ns="ns",
        checkpoint_id="cp1",
        task_id="task1",
        idx=0,
        channel="ch1",
        type="dummy",
        value="some_value"
    )
    d = write.to_dict()
    expected_partition = Write.separator.join(["t1", "cp1", "ns"])
    expected_sort = Write.separator.join(["task1", "0"])
    assert d["partition_key"] == expected_partition
    assert d["sort_key"] == expected_sort
    assert d["channel"] == "ch1"
    assert d["type"] == "dummy"
    assert d["value"] == "some_value"
    # Test the unique entity key.
    expected_entity_key = f"{expected_partition}{Write.separator}{expected_sort}"
    assert write.get_entity_key() == expected_entity_key

def test_from_datastore_entity():
    partition_key = Write.separator.join(["t1", "cp1", "ns"])
    sort_key = Write.separator.join(["task1", "0"])
    entity = {
        "partition_key": partition_key,
        "sort_key": sort_key,
        "channel": "ch1",
        "type": "dummy",
        "value": "some_value"
    }
    write = Write.from_datastore_entity(entity)
    assert write.thread_id == "t1"
    assert write.checkpoint_id == "cp1"
    assert write.checkpoint_ns == "ns"
    assert write.task_id == "task1"
    assert write.idx == 0
    assert write.channel == "ch1"
    assert write.type == "dummy"
    assert write.value == "some_value"
