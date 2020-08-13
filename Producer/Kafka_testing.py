#!/usr/bin/env python
# coding: utf-8

# In[7]:


import gc
import platform
import time
import threading
import pytest
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
import logging
from kafka.errors import KafkaError
import hashlib


# In[ ]:





# In[9]:


if __name__ == "__main__":
    logging.basicConfig(filename="logFile.log", level=logging.INFO)
    connect_str = 'localhost:9092'
    producer = KafkaProducer(bootstrap_servers=connect_str,
                             retries=5,
                             max_block_ms=30000,
                             value_serializer=str.encode)
    #topic = random_string(5)
    topic = 'JivWn'
    messages = 100
    futures = []
    for i in range(messages):
        futures.append(producer.send(topic, 'msg %d' % i))
    try:
        ret = [f.get(timeout=30) for f in futures]
        logging.info(ret[-1].offset)
    except KafkaError:
        producer.close()
        logging.info(KafkaError)
    assert len(ret) == messages
    logging.info("All messages inserted successfully")
    producer.close()
    consumer = KafkaConsumer(bootstrap_servers=connect_str,
                             group_id='my-group',
                             consumer_timeout_ms=30000,
                             auto_offset_reset='earliest',
                             value_deserializer=bytes.decode)
    consumer.subscribe([topic])
    msgs = set()
    for messages in consumer:
        try:
            msgs.add(messages.offset)
        except StopIteration:
            consumer.close()
            break
    consumer.close()
    assert ret[-1].offset == max(msgs)
    #max(msgs)
    #assert ret[-1].offset == max(msgs)


# In[10]:




