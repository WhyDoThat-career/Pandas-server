import pymysql
from pymysql.cursors import DictCursor
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json
import pandas as pd

class MySQL :
    def __init__(self,key_file,database) :
        KEY = self.load_key(key_file)
        self.MYSQL_CONN = pymysql.connect(
            host = KEY['host'],
            user=KEY['user'],
            passwd=KEY['password'],
            db=database,
            charset='utf8mb4',
            cursorclass=DictCursor
        )
    def load_key(self,key_file) :
        with open(key_file) as key_file :
            key = json.load(key_file)
        return key

    def conn_mysqldb(self):
        if not self.MYSQL_CONN.open :
            self.MYSQL_CONN.ping(reconnect=True)
        return self.MYSQL_CONN

    def get_dataframe(self,table) :
        db = self.conn_mysqldb()
        cusor = db.cursor()
        sql_query = f'SELECT * FROM {table}'
        db.execute(sql_query)
        return pd.DataFrame(db.fetchall()).drop(['id'],axis='columns')

class Kafka(KafkaConsumer) :
    def __init__(self,keyfile,topic) :
        KEY = self.load_key(key_file)
        self.stream = True
        self.consumer = KafkaConsumer(topic, 
                            bootstrap_servers=[f'{KEY['host']}:{KEY['port']}'],
                            auto_offset_reset='earliest',
                            enable_auto_commit=True,
                            group_id='my-group',
                            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                            )

    def load_key(self,key_file) :
        with open(key_file) as key_file :
            key = json.load(key_file)
        return key
    
    def stream_data(self) :
        if self.stream :
            for message in self.consumer :
                data = message.value['Message']
                datetime = message.value['Asctime']
                if "activity" in data :
                    table_name = data['activity']
                    sql_dict = {
                            "user_id" : data['user_id'],
                            "datetime" : datetime.split(',')[0]
                        }
                    if table_name == 'resume_sector' :
                        sql_dict['sector_id'] = data['resume_select']
                    elif table_name == 'resume_skill' :
                        sql_dict['skill_id'] = data['resume_select']
                    elif table_name == 'filtering' :
                        sql_dict['filtering'] = data['filter_text']
                    else :
                        sql_dict['recruit_id'] = data['recruit_id']

                    mysql.insert_data(table_name,sql_dict,dtype_map)
                    print(f"[SQL INPUT]Time : {datetime}, Key: {message.key}, Value: {data}")
    
    def close_stream(self) :
        self.stream = False

class ELK(Elasticsearch) :
    def __init__(self,keyfile,index) :
        pass
    def load_key(self,key_file) :
        with open(key_file) as key_file :
            key = json.load(key_file)
        return key