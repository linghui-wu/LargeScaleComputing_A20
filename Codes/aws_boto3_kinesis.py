import boto3

# Create a Kinesis stream
kinesis = boto3.client("kinesis")

kinesis.create_stream(
    StreamName="stream_name",
    ShardCount=1  # Only 1 shard for AWS Educate - 1M input and 2M output
    )

# Put data into a Kinesis stream
kinesis.put_record(
    StreamName="stream_name",
    Data=data,
    PartitionKey="patitionkey"
    )

# Get data from a Kinesis stream
shard_it = kinesis.get_shard_iterator(
    StreamName="stream_name",
    ShardId="shardId-000000000000",
    ShardIteratorType="LATEST"
    )["ShardIterator"]

while 1 == 1:
    out = kinesis.get_records(ShardIterator=shard_it, Limit=1)

    # Do something with streaming data

    # Set shard iterator to next position in the data stream
    shard_it = out["NextShardIterator"]

