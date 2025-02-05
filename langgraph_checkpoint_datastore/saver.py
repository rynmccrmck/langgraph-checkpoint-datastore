from google.cloud import datastore
from typing import Any, Dict, Iterator, Optional, Sequence, Tuple

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    BaseCheckpointSaver,
    CheckpointTuple,
    Checkpoint,
    CheckpointMetadata,
    ChannelVersions,
)
from langgraph.checkpoint.serde.base import SerializerProtocol

from langgraph_checkpoint_datastore.write import Write


class DatastoreSaver(BaseCheckpointSaver):
    def __init__(
        self,
        *,
        project: Optional[str] = None,
        client_config: Optional[Dict[str, Any]] = None,
        serde: Optional[SerializerProtocol] = None,
        # The names of the “kinds” for checkpoints and writes.
        checkpoints_kind: str = "Checkpoint",
        writes_kind: str = "Write",
    ) -> None:
        super().__init__(serde=serde)
        # Pass project and any extra client parameters.
        self.client = datastore.Client(project=project, **(client_config or {}))
        self.checkpoints_kind = checkpoints_kind
        self.writes_kind = writes_kind

    def _checkpoint_key(self, configurable: Dict[str, Any]) -> datastore.Key:
        """Compose a Datastore key for a checkpoint.
        
        Here we use a composite key name built from thread_id, checkpoint_ns, and checkpoint_id.
        """
        thread_id = configurable["thread_id"]
        checkpoint_ns = configurable.get("checkpoint_ns", "")
        checkpoint_id = configurable["checkpoint_id"]
        key_name = f"{thread_id}:::{checkpoint_ns}:::{checkpoint_id}"
        return self.client.key(self.checkpoints_kind, key_name)

    def get_tuple(self, config: RunnableConfig) -> Optional[CheckpointTuple]:
        configurable = self.validate_configurable(config.get("configurable"))
        entity = self._get_entity(configurable)
        if not entity:
            return None

        # Deserialize the checkpoint and metadata.
        type_ = entity.get("type")
        checkpoint_serialized = entity.get("checkpoint")
        metadata_serialized = entity.get("metadata")
        checkpoint = self.serde.loads_typed([type_, checkpoint_serialized])
        metadata = self.serde.loads_typed([type_, metadata_serialized])

        # Fetch pending writes: query Write entities where the partition_key matches.
        partition_key = Write.compute_partition_key({
            "thread_id": entity.get("thread_id"),
            "checkpoint_id": entity.get("checkpoint_id"),
            "checkpoint_ns": entity.get("checkpoint_ns", ""),
        })  
        query = self.client.query(kind=self.writes_kind)
        query.add_filter("partition_key", "=", partition_key)
        write_entities = list(query.fetch())
        pending_writes = []
        for write_entity in write_entities:
            write = Write.from_datastore_entity(write_entity)
            value = self.serde.loads_typed([write.type, write.value])
            pending_writes.append((write.task_id, write.channel, value))

        config_out = {
            "configurable": {
                "thread_id": entity.get("thread_id"),
                "checkpoint_ns": entity.get("checkpoint_ns", ""),
                "checkpoint_id": entity.get("checkpoint_id"),
            }
        }
        parent_config = None
        if entity.get("parent_checkpoint_id"):
            parent_config = {
                "configurable": {
                    "thread_id": entity.get("thread_id"),
                    "checkpoint_ns": entity.get("checkpoint_ns", ""),
                    "checkpoint_id": entity.get("parent_checkpoint_id"),
                }
            }
        return CheckpointTuple(
            config=config_out,
            checkpoint=checkpoint,
            metadata=metadata,
            parent_config=parent_config,
            pending_writes=pending_writes,
        )

    def _get_entity(self, configurable: Dict[str, Any]) -> Optional[datastore.Entity]:
        if configurable["checkpoint_id"] is not None:
            key = self._checkpoint_key(configurable)
            return self.client.get(key)
        else:
            # If no checkpoint_id was provided, query for the most recent checkpoint for the thread.
            query = self.client.query(kind=self.checkpoints_kind)
            query.add_filter("thread_id", "=", configurable["thread_id"])
            if configurable.get("checkpoint_ns"):
                query.add_filter("checkpoint_ns", "=", configurable["checkpoint_ns"])
            # Order descending by checkpoint_id (adjust if you store a timestamp instead)
            query.order = ["-checkpoint_id"]
            entities = list(query.fetch(limit=1))
            return entities[0] if entities else None

    def list(
        self,
        config: Optional[RunnableConfig],
        *,
        filter: Optional[Dict[str, Any]] = None,
        before: Optional[RunnableConfig] = None,
        limit: Optional[int] = None,
    ) -> Iterator[CheckpointTuple]:
        configurable = config.get("configurable", {})
        thread_id = configurable.get("thread_id")
        query = self.client.query(kind=self.checkpoints_kind)
        query.add_filter("thread_id", "=", thread_id)
        if before and before.get("configurable") and before["configurable"].get("checkpoint_id"):
            # Filter to only those checkpoints that sort before the given one.
            before_checkpoint_id = before["configurable"]["checkpoint_id"]
            query.add_filter("checkpoint_id", "<", before_checkpoint_id)
        query.order = ["-checkpoint_id"]
        query_iter = query.fetch(limit=limit) if limit else query.fetch()
        for entity in query_iter:
            type_ = entity.get("type")
            checkpoint = self.serde.loads_typed([type_, entity.get("checkpoint")])
            metadata = self.serde.loads_typed([type_, entity.get("metadata")])
            config_out = {
                "configurable": {
                    "thread_id": entity.get("thread_id"),
                    "checkpoint_ns": entity.get("checkpoint_ns", ""),
                    "checkpoint_id": entity.get("checkpoint_id"),
                }
            }
            parent_config = None
            if entity.get("parent_checkpoint_id"):
                parent_config = {
                    "configurable": {
                        "thread_id": entity.get("thread_id"),
                        "checkpoint_ns": entity.get("checkpoint_ns", ""),
                        "checkpoint_id": entity.get("parent_checkpoint_id"),
                    }
                }
            yield CheckpointTuple(
                config=config_out,
                checkpoint=checkpoint,
                metadata=metadata,
                parent_config=parent_config,
            )

    def put(
    self,
    config: RunnableConfig,
    checkpoint: Checkpoint,
    metadata: CheckpointMetadata,
    new_versions: ChannelVersions,
) -> RunnableConfig:
        configurable = self.validate_configurable(config.get("configurable"))
        thread_id = configurable["thread_id"]
        type1, serialized_checkpoint = self.serde.dumps_typed(checkpoint)
        type2, serialized_metadata = self.serde.dumps_typed(metadata)
        if type1 != type2:
            raise ValueError("Failed to serialize checkpoint and metadata to the same type.")
    
        # Create a new configurable dict that uses the new checkpoint id
        new_configurable = dict(configurable)
        new_configurable["checkpoint_id"] = checkpoint.get("id")
        key = self._checkpoint_key(new_configurable)
        
        entity = datastore.Entity(key=key)
        entity.update({
            "thread_id": thread_id,
            "checkpoint_ns": config.get("configurable", {}).get("checkpoint_ns", ""),
            "checkpoint_id": checkpoint.get("id"),
            "parent_checkpoint_id": config.get("configurable", {}).get("checkpoint_id"),
            "type": type1,
            "checkpoint": serialized_checkpoint,
            "metadata": serialized_metadata,
        })
        self.client.put(entity)
        return {
            "configurable": {
                "thread_id": entity["thread_id"],
                "checkpoint_ns": entity["checkpoint_ns"],
                "checkpoint_id": entity["checkpoint_id"],
            }
        }

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[Tuple[str, Any]],
        task_id: str,
    ) -> None:
        configurable = self.validate_configurable(config.get("configurable"))
        thread_id = configurable["thread_id"]
        checkpoint_ns = configurable.get("checkpoint_ns", "")
        checkpoint_id = configurable.get("checkpoint_id")
        if checkpoint_id is None:
            raise ValueError("Missing checkpoint_id")
        write_entities = []
        for idx, write in enumerate(writes):
            channel, value = write
            type_, serialized_value = self.serde.dumps_typed(value)
            write_obj = Write(
                thread_id=thread_id,
                checkpoint_ns=checkpoint_ns,
                checkpoint_id=checkpoint_id,
                task_id=task_id,
                idx=idx,
                channel=channel,
                type=type_,
                value=serialized_value,
            )
            # Create a key for the write entity. We build a unique key name from its partition and sort keys.
            key = self.client.key(self.writes_kind, write_obj.get_entity_key())
            entity = datastore.Entity(key=key)
            entity.update(write_obj.to_dict())
            write_entities.append(entity)
        self.client.put_multi(write_entities)

    def validate_configurable(self, configurable):
        if not configurable:
            raise ValueError("Missing configurable")
        thread_id = configurable.get("thread_id")
        checkpoint_ns = configurable.get("checkpoint_ns", "")
        checkpoint_id = configurable.get("checkpoint_id")
        if not isinstance(thread_id, str):
            raise ValueError("Invalid thread_id")
        if not (isinstance(checkpoint_ns, str) or checkpoint_ns is None):
            raise ValueError("Invalid checkpoint_ns")
        if not (isinstance(checkpoint_id, str) or checkpoint_id is None):
            raise ValueError("Invalid checkpoint_id")
        return {
            "thread_id": thread_id,
            "checkpoint_ns": checkpoint_ns or "",
            "checkpoint_id": checkpoint_id,
        }
