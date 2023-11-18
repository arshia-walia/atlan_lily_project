import json
from pykafka import KafkaClient
import threading
import time




class ReadJsonFile:
    def __init__(self):
        self.KAFKA_HOST = "localhost:9092"
        self.client = KafkaClient(hosts=self.KAFKA_HOST)
        self.topic = self.client.topics["ArshiaWaliaTest"]
        
    def read_file(self, file_name):
        client_input = input("enter the inputs for the given alerts:\n 'normal_output': 1,\n'lag': 2,\n'schema_error': 3,\n'row_validation': 4  ")
        f = open(file_name)
        data = json.load(f)
        with self.topic.get_sync_producer() as producer:
            while True: #to make big file of json objects
                for object in data["data"]:
                    if client_input == 'row_validation' :
                        self.row_validation_error(object)
                    elif client_input == 'lag':
                        self.manual_delay(5)
                    elif client_input == 'schema_error':
                        self.schema_change_error(object)
                    message = str(object)
                    encoded_message = message.encode("utf-8")
                    producer.produce(encoded_message)
                time.sleep(10)
            f.close()
    
    
    def manual_delay(self, delay_duration_in_seconds):
        time.sleep(delay_duration_in_seconds)
    
    def schema_change_error(self, data):
        data["quantity"] = str(data["quantity"])
    
    def row_validation_error(self, data):
        data["quantity"] = -abs(int(data["quantity"]))

ReadJsonFile().read_file("sample1.json")
