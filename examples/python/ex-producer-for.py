from confluent_kafka import Producer

broker = 'peter-kafka001.foo.bar, peter-kafka002.foo.bar, peter-kafka003.foo.bar'
topic = 'peter-topic'
# Producer configuration
# See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
conf = {'bootstrap.servers': broker, 'compression.codec': 'gzip', 'acks': 1}

p = Producer(**conf)
# for loop
for messageCount in range(1, 11):
    data = 'Apache Kafka is a distributed streaming platform - %d' % messageCount
    p.produce(topic, data.encode('utf-8'))
p.flush()