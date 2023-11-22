from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
import json
import json_lines

# create an instance of a Kafka Admin Client
# client = KafkaAdminClient(bootstrap_servers='localhost:9092')
#
# # Create a NewTopic object which holds the configuration for our new Kafka Topic.
# books_topic = NewTopic(name='best_books', num_partitions=2, replication_factor=1)
# client.create_topics(books_topic)

# The key_serializer and value_serializer is required because we need to send data to kafka in
# bytes OR in a type that can be serialized (ASCII, UTF-8, etc).
# We turn it to JSON (encoded in UTF-8) since it's a common message format

producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092',
                         acks=1,
                         api_version = (0,7,2,1),
                         key_serializer=lambda v: json.dumps(v).encode('utf-8'),
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Iterating through the json lines file
with open('sample.json', 'rb') as f:
    for item in json_lines.reader(f):
        # Add a print statement to see messages being read in from the JSON file.
        # print(item['bookID'])

        # these messages are being processed synchronously since we're waiting for a response
        # and also providing a timeout of 100 millisecnds

        future = producer.send(topic='crashes', key=item['collision_id'], value=item)
        result = future.get(timeout=200)

# Closing the file handle
f.close()

