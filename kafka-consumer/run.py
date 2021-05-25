from kafka_server import Kafka

kafka = Kafka(key_file='keys/aws_kafka_key.json',topic='flask_all_logs')

if __name__ == '__main__' :
    kafka.run()