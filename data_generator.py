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
        mapper = {'normal_output': 1, 
                  'lag': 2, 
                  'schema_error': 3, 
                  'row_validation': 4
                  }
        client_input = int(input("choose type of data generation:\n {}".format(mapper)))
        f = open(file_name)
        data = json.load(f)
        with self.topic.get_sync_producer() as producer:
            while True: #to make big file of json objects
                for object in data["data"]:
                    if client_input == mapper['normal_output'] :
                        pass
                    elif client_input == mapper['lag']:
                        self.manual_delay(5)
                    elif client_input == mapper['schema_error']:
                        self.schema_change_error(object)
                    elif client_input == mapper['row_validation'] :
                        self.row_validation_error(object)
                    message = str(object)
                    encoded_message = message.encode("utf-8")
                    producer.produce(encoded_message)
                    
                    time.sleep(3)
            f.close()


    def manual_delay(self, delay_duration_in_seconds):
        time.sleep(delay_duration_in_seconds)

    def schema_change_error(self, data):
        data["age"] = str(data["age"])

    def row_validation_error(self, data):
        data["age"] = -abs(int(data["age"]))

ReadJsonFile().read_file("sample1.json")
