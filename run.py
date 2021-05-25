#To Do
#Docker에서 실행할 run.py
from typing import Optional
from fastapi import FastAPI,Request
from pydantic import BaseModel
from Data_server.kafka_server import KafkaThread
from connector import Kafka, Redis
import logging
import time
import random

app = FastAPI()
kafka = {'user':None,'activity':{}}
user_session = Redis()

def recommand(user) :
    return [random.randint(1,5000),random.randint(1,5000)]

@app.post('/kafka')
async def consumer(request: Request) :
    global kafka, user_session
    kafka = await request.json()
    if 'info' in kafka['activity'] :
        if kafka['activity']['info'] == 'Login' :
            user_id = kafka['user']
            user_session.insert(user_id,recommand(user_id))
        elif kafka['activity']['info'] == 'Logout' :
            user_id = kafka['user']
            try :
                user_session.delete(user_id)
            except :
                print('Already delete data')
                pass
    return True

@app.get("/recommand")
def read_reco():
    global kafka, user_session
    print('read_reco :',kafka)
    return kafka,user_session.get_all()