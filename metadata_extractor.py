from datetime import timedelta, datetime
from kafka import KafkaConsumer
import ast


class RealTimeStreaming:
    
    def __init__(self):
        self.KAFKA_TOPIC = "ArshiaWaliaTest"
        self.KAFKA_SERVER = "localhost:9092"
    
    def kafka_consumer(self):
        start_time = datetime.now()
        consumer = KafkaConsumer(self.KAFKA_TOPIC, bootstrap_servers=[self.KAFKA_SERVER], auto_offset_reset='latest')
        for event in consumer:
            print("\nEVENT recieved : {}".format(event))
            
            event = ast.literal_eval(event.value.decode('utf-8'))
            print("event details: {}".format(event))
            
            if self.is_event_valid(event) and not self.is_delay(start_time, event):
                print("event successfully consumed. NO_DELAY_DETECTED | NO_SCHEMA_ERROR | NO_ROW_INVALIDATE")
            start_time = datetime.now()
            
        
    def is_event_valid(self, event):
        return self.is_schema_valid(event) and self.is_row_valid(event)
                         
        
    
    def is_schema_valid(SELF, event):
        try:
            if (not "age" in event) or (type(event["age"]) != int):
                print("\n[ALERT][SCHEMA_ERROR] details: DataType mismatch for $.age Expected int, recieved {}".format(type(event["age"])))
                return False
            else: return True
        except Exception as e:
            print("SCHEMA ERROR, exception details: {} ".format(e))
            return False

            
            
    def is_delay(self, start_time, event):
        end_time = datetime.now()
        if end_time - start_time > timedelta(seconds=3):
            print("\n[ALERT] delay identified: {}".format(end_time - start_time))
            return True
        else: return False
            
    
    def is_row_valid(self, event):
        if event["age"] < 0:
            print("\n[ALERT][ROW_INVALIDATE] details: value for $.age Expected positive int, recieved: {}".format(event["age"]))
            return False
        else: return True
            
            
while True:
    RealTimeStreaming().kafka_consumer()
