import pymysql
from pymysql.cursors import DictCursor
from kafka import KafkaConsumer
import json
import pandas as pd
import redis

class MySQL :
    def __init__(self,key_file,database) :
        KEY = self.load_key(key_file)
        self.MYSQL_CONN = pymysql.connect(
            host = KEY['host'],
            user=KEY['user'],
            passwd=KEY['password'],
            db=database,
            charset='utf8mb4',
            cursorclass=DictCursor,
            autocommit=True
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
        cusor.execute(sql_query)
        return pd.DataFrame(cusor.fetchall())
    
    def update_data(self,table,items) :
        db = self.conn_mysqldb()
        cursor = db.cursor()


class Kafka :
    def __init__(self,key_file,topic) :
        KEY = self.load_key(key_file)
        self.consumer = KafkaConsumer(topic, 
                            bootstrap_servers=[f'{KEY["host"]}:{KEY["port"]}'],
                            auto_offset_reset='earliest',
                            enable_auto_commit=True,
                            group_id='my-group',
                            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                            )

    def load_key(self,key_file) :
        with open(key_file) as key_file :
            key = json.load(key_file)
        return key
    
class Redis :
    def __init__(self) :
        self.session = redis.StrictRedis(host='redis', port=6379, db=0)
        print('Redis Connecting')

    def insert(self,user_id,data) :
        json_data = json.dumps(data,ensure_ascii=False).encode('utf-8')
        self.session.set(user_id,json_data)
        print(f'Create Session {user_id} recommand {data}')
    
    def delete(self,user_id) :
        self.session.delete(user_id)
        print(f'Delete Session {user_id}')

    def get(self,user_id) :
        return json.loads(self.session.get(user_id).decode('utf-8'))
        
    def get_all(self) :
        placeholder = dict()
        keys = self.session.keys()
        values = self.session.mget(keys)
        return dict(zip(keys,values))
