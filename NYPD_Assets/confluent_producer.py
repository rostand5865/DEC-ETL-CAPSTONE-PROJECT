import json
import json_lines
import sys

from confluent_kafka import Producer

def read_ccloud_config(config_file):
    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                conf[parameter] = value.strip()
    return conf

def delivery_callback(err, msg):
    if err:
        sys.stderr.write('%% Message failed delivery: %s\n' % err)
    else:
        sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                            (msg.topic(), msg.partition(), msg.offset()))

producer = Producer(read_ccloud_config("client.properties"))

# Iterating through the json lines file
with open('sample.json', 'rb') as f:
    for item in json_lines.reader(f):
        producer.produce(
            topic="crashes", 
            key=json.dumps(item['collision_id']).encode('utf-8'), 
            value=json.dumps(item).encode('utf-8'),
            callback=delivery_callback
        )
        producer.poll(0)

# Closing the file handle
producer.flush()
f.close()
