#To Do
#Kafka consumer를 통해 kafka stream을 가져옵니다.
from kafka import KafkaConsumer
import threading
import requests
import json
import time
class Kafka :
    def __init__(self,key_file,topic) :
        KEY = self.load_key(key_file)
        self.consumer = KafkaConsumer(topic, 
                            bootstrap_servers=[f'{KEY["host"]}:{KEY["port"]}'],
                            auto_offset_reset='earliest',
                            enable_auto_commit=True,
                            group_id='recommend_server',
                            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                            )
        self._stop = False
        self.activity = {'user':None,'activity':{}}
        self.headers = {'Content-Type': 'application/json; charset=utf-8'}

    def load_key(self,key_file) :
        with open(key_file) as key_file :
            key = json.load(key_file)
        return key


    def run(self) :
        print('run kafka')
        if not self._stop :
            for message in self.consumer :
                data = message.value['Message']
                datetime = message.value['Asctime']
                user_id = message.value['User']
                if "activity" in data :
                    ret_data = {
                        'user' : user_id,
                        'activity' : data
                    }
                    print(ret_data,end=', response..') 
                    res = requests.post('http://pandas-server:8080/kafka',data=json.dumps(ret_data),
                     headers=self.headers)
                    print(res)
                elif "info" in data and (data['info']=='Login' or data['info']=='Logout') :
                    ret_data = {
                        'user' : user_id,
                        'activity' : data
                    }
                    print(ret_data,end=', response..') 
                    res = requests.post('http://pandas-server:8080/kafka',data=json.dumps(ret_data),
                     headers=self.headers)
                    print(res)
                # time.sleep(0.1)
            
    def close(self) :
        print('Close Stream')
        self._stop = True