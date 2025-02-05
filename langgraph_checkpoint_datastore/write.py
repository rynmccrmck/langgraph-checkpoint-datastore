from typing import Any

class Write:
    separator = ":::"

    def __init__(
        self,
        *,
        thread_id: str,
        checkpoint_ns: str,
        checkpoint_id: str,
        task_id: str,
        idx: int,
        channel: str,
        type: str,
        value: Any,
    ):
        self.thread_id = thread_id
        self.checkpoint_ns = checkpoint_ns
        self.checkpoint_id = checkpoint_id
        self.task_id = task_id
        self.idx = idx
        self.channel = channel
        self.type = type
        self.value = value

    def to_dict(self):
        """Return a dictionary representation of this write for storing in Datastore."""
        return {
            "partition_key": self.get_partition_key(),
            "sort_key": self.get_sort_key(),
            "channel": self.channel,
            "type": self.type,
            "value": self.value,  # Adjust if you need to convert binary data, etc.
        }

    def get_entity_key(self) -> str:
        """Compose a unique key name for this write entity."""
        return f"{self.get_partition_key()}{self.separator}{self.get_sort_key()}"

    def get_partition_key(self) -> str:
        # Instance method: uses the object's attributes.
        return self.separator.join([self.thread_id, self.checkpoint_id, self.checkpoint_ns])

    def get_sort_key(self) -> str:
        return self.separator.join([self.task_id, str(self.idx)])

    @classmethod
    def from_datastore_entity(cls, entity):
        partition_key = entity.get("partition_key")
        parts = partition_key.split(cls.separator)
        thread_id = parts[0]
        checkpoint_id = parts[1]
        checkpoint_ns = parts[2] if len(parts) > 2 else ""
        sort_key = entity.get("sort_key")
        sort_parts = sort_key.split(cls.separator)
        task_id = sort_parts[0]
        idx = int(sort_parts[1])
        channel = entity.get("channel")
        type_ = entity.get("type")
        value = entity.get("value")
        return cls(
            thread_id=thread_id,
            checkpoint_ns=checkpoint_ns,
            checkpoint_id=checkpoint_id,
            task_id=task_id,
            idx=idx,
            channel=channel,
            type=type_,
            value=value,
        )

    @staticmethod
    def compute_partition_key(item: dict) -> str:
        """Helper to create a partition key from a dictionary containing thread_id, checkpoint_id, and checkpoint_ns."""
        return Write.separator.join(
            [item["thread_id"], item["checkpoint_id"], item.get("checkpoint_ns", "")]
        )

    # Static alias for backward compatibility
    @staticmethod
    def get_partition_key_from_item(item: dict) -> str:
        return Write.compute_partition_key(item)
