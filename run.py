#To Do
#Docker에서 실행할 run.py
from typing import Optional
from fastapi import FastAPI,Request
from pydantic import BaseModel
from Data_server.kafka_server import KafkaThread
from connector import Kafka, Redis
from Reco_engine.recommend import Recommend
from fastapi.responses import JSONResponse
import logging
import time
import random

reco_engine = Recommend()
app = FastAPI()
kafka = {'user':None,'activity':{}}
user_session = Redis()

@app.post('/kafka')
async def consumer(request: Request) :
    global kafka, user_session, reco_engine
    kafka = await request.json()
    if kafka['activity']['info'] == 'Logout' :
            user_id = kafka['user']
            try :
                user_session.delete(user_id)
            except :
                print('Already delete data')
                pass
    else :
        user_id = kafka['user']
        user_session.insert(user_id,reco_engine.recommend(user_id))
    return True

@app.get("/recommend")
async def read_reco(user_id : str):
    global kafka, user_session
    recommend_dict = {
        'user_id' : user_id,
        'recommend' : user_session.get(user_id)
    }
    print('Send recommend',recommend_dict)
    return JSONResponse(recommend_dict)