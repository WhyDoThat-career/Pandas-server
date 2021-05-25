#To Do
#Kafka consumer를 통해 kafka stream을 가져옵니다.
from connector import Kafka
import threading

class KafkaThread(Kafka) :
    def __init__(self) :
        super().__init__(key_file='keys/aws_kafka_key.json',topic='flask_all_logs')
        self._stop = False
        self._data_ready = threading.Event()
        self._thread = threading.Thread(target=self._run,args=())
        self._thread.daemon = True
        self._thread.start()
        self.activity = {'user':None,'activity':{}}

    def _run(self) :
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
                    print(ret_data) 
                    self.activity = ret_data
                elif "info" in data and (data['info']=='Login' or data['info']=='Logout') :
                    ret_data = {
                        'user' : user_id,
                        'activity' : data
                    }
                    print(ret_data) 
                    self.activity = ret_data
            
    def close(self) :
        print('Close Stream')
        self._stop = True