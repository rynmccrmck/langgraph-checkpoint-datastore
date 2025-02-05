# langgraph-checkpoint-datastore

Implementation of a LangGraph CheckpointSaver that uses a GCP's Datastore

## Inspiration

Based on: https://github.com/justinram11/langgraph-checkpoint-dynamodb

## Required Datastore Tables

To be able to use this checkpointer, two Datastore kinds are needed, one to store
checkpoints and the other to store writes.

## Using the Checkpoint Saver

### Default

To use the datastore checkpoint saver, you only need to specify the names of
the checkpoints and writes kinds. In this scenario the datastore client will
be instantiated with the default configuration, great for running on AWS Lambda.

```python
from langgraph_checkpoint_datastore import DatastoreSaver
...
checkpoints_table_name = 'YourCheckpointsTableName'
writes_table_name = 'YourWritesTableName'

memory = DatastoreSaver(
    checkpoints_table_name=checkpoints_table_name,
    writes_table_name=writes_table_name,
)

graph = workflow.compile(checkpointer=memory)
```

### Providing Client Configuration

If you need to provide custom configuration to the datastore client, you can
pass in an object with the configuration options. Below is an example of how
you can provide custom configuration.

```python
memory = DatastoreSaver(
    checkpoints_table_name=checkpoints_table_name,
    writes_table_name=writes_table_name,
    client_config={
        'region': 'us-west-2',
    }
)
```

Important note, you will need to add an index to your kinds:

````indexes:

- kind: IntegrationCheckpoint
  properties:
  - name: thread_id
  - name: checkpoint_id
    direction: desc```
````
