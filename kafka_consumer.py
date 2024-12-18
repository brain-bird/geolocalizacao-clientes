#python3 kafka_consumer.py

from kafka import KafkaConsumer
import fastavro
import json

# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer('subscribers',
                         group_id='my-group',
                         bootstrap_servers=['localhost:9092'])

for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`

    #se o um usuario esta dentro da regiao onde ha promocao
    message_json = json.loads(message.value)
    #chama a API de envio de SMS
    print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, 'MSISDN',
                                          message_json['MSISDN']))