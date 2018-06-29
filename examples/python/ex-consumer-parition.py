from confluent_kafka import Consumer, KafkaException, KafkaError, TopicPartition

broker = 'peter-kafka001.foo.bar, peter-kafka002.foo.bar, peter-kafka003.foo.bar'
group = 'peter-group'

# Consumer configuration
# See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
conf = {'bootstrap.servers': broker, 'group.id': group,
        'default.topic.config': {'auto.offset.reset': 'smallest'}}
c = Consumer(conf)
# topic and partition
c.assign([TopicPartition('peter-topic', 0)])

# Read messages from Kafka, print to stdout
try:
    while True:
        msg = c.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            # Error or event
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                print('Topic: %s, Partition: %d, Next Offset: %d') % (msg.topic(), msg.partition(), msg.offset())
            else:
                # Error
                raise KafkaException(msg.error())
        else:
            print('Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s' % (
            msg.topic(), msg.partition(), msg.offset(), str(msg.key()), msg.value().decode('utf-8')))
except KeyboardInterrupt:
    print('## Aborted by user\n')

finally:
    # Close down consumer to commit final offsets.
    c.close()