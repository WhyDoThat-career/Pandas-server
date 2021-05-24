#To Do
#Docker에서 실행할 run.py
from typing import Optional
from fastapi import FastAPI
from Data_server.kafka_server import KafkaThread
from connector import Kafka
import logging
import time


app = FastAPI()
kafka = KafkaThread()
user_session = dict()

def recommand(user) :
    return ['32','52']

@app.get("/")
def read_root():
    if 'info' in kafka.activity['activity'] :
        if kafka.activity['activity']['info'] == 'Login' :
            user_id = kafka.activity['user']
            user_session[user_id] = recommand(user_id)
        elif kafka.activity['activity']['info'] == 'Logout' :
            user_id = kafka.activity['user']
            try :
                del user_session[user_id]
            except KeyError :
                pass
    return kafka.activity,user_session

@app.get("/items/{item_id}")
def read_item(item_id: int, q: Optional[str] = None):
    return {"item_id": item_id, "q": q}
