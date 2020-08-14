#!/usr/bin/env python
# coding: utf-8

# In[35]:


from kafka import KafkaProducer,KafkaClient
from kafka.errors import KafkaError
import time
import json
import websocket
import logging
import re
#LOCAL IMPORTS
import config as myCred
from confluent_kafka.admin import AdminClient
import hashlib


# In[54]:


def onOpen(ws):
    print("connection Opened")
    auth_data={
        "action": "authenticate",
        "data": {"key_id": myCred.API_KEY, "secret_key": myCred.SECRET_KEY}
    }
    
    ws.send(json.dumps(auth_data))
    
    listenMsg = {
        "action": "listen", 
        "data": {"streams": ["T.TSLA","T.AAPL","T.MSFT","T.QCOM"]}
    }

    ws.send(json.dumps(listenMsg))
    
def onMsg(ws,message):
    if re.match('^.*authorized.*$',message):
        print(message)
        return
    else:
        if(time.time()-startTime)<30:
            print(message)
            try:
                producer.send(topicName, value=message.encode('utf-8'))
                return True
            except Exception as ex:
                logging.error(str(ex))
        else:
            producer.close()
            ws.close()
            
def onClose(ws):
    print("connection Closed")
    
def testCode():
    i=0
    while i<30:
        x = '{"stream":"T.AAPL","data":{"ev":"T","T":"AAPL","i":"3041","x":2,"p":389.32,"s":1,"t":1596126853339000000,"c":[37],"z":3},"loop":'+str(i)+'}' 
        print(x)
        producer.send(topicName, value=x.encode("utf-8"))
        i=i+1
    producer.close()


# In[57]:


if __name__ == "__main__":
    #creating producer log file
    logging.basicConfig(filename="producer_log.log", level=logging.INFO)
    
    #unit test to check if zookeeper an kafka are running properly
    conf = {'bootstrap.servers':'localhost:9092'}
    admin_client = AdminClient(conf)
    topics = admin_client.list_topics().topics
    if not topics: 
        logging.error(RuntimeError())
    else:
        logging.info(topics)
    #websocket for data stream
    startTime =time.time()
    socket = "wss://data.alpaca.markets/stream"
    ws = websocket.WebSocketApp(socket,on_open=onOpen, on_close=onClose, on_message=onMsg)
    
    try:
        producer=KafkaProducer(bootstrap_servers=" localhost:9092")
        topicName="test-topic"
        #testCode()
    except KafkaError:
        logging.error(KafkaError+'\n')
    ws.run_forever()


# In[ ]:





# In[ ]:




