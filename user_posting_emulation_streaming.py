import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
import yaml


random.seed(100)


class AWSDBConnector:

    def __init__(self):
        pass

    def read_db_creds(self):
        """
        Reads and Returns the database Credentials from the YAML file (not included in Git Repo).
        """

        with open("db_creds.yaml", "r") as f:
            creds = yaml.safe_load(f)
            return creds
        
    def create_db_connector(self):
        creds = self.read_db_creds()
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{creds['USER']}:{creds['PASSWORD']}@{creds['HOST']}:{creds['PORT']}/{creds['DATABASE']}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()


def run_infinite_post_data_loop():

    while True :
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
            
            pin_payload = json.dumps({
                "StreamName": "streaming-0e95b18877fd-pin",
                "records": {
                        "index": pin_result["index"],
                        "unique_id": pin_result["unique_id"],
                        "title": pin_result["title"],
                        "description": pin_result["description"],
                        "poster_name": pin_result["poster_name"],
                        "follower_count": pin_result["follower_count"],
                        "tag_list": pin_result["tag_list"],
                        "is_image_or_video": pin_result["is_image_or_video"],
                        "image_src": pin_result["image_src"],
                        "downloaded": pin_result["downloaded"],
                        "save_location": pin_result["save_location"],
                        "category": pin_result["category"]
                    },
                    "PartitionKey": "pintrest_test" 
            })
            headers = {'Content-Type': 'application/json'}
            pin_response = requests.request("PUT","https://jl1y5b7yai.execute-api.us-east-1.amazonaws.com/dev/streams/streaming-0e95b18877fd-pin/record", headers=headers, data=pin_payload)
            print(pin_payload, pin_response.json(), pin_response.status_code) 

            geo_payload = json.dumps({
                "StreamName": "streaming-0e95b18877fd-geo",
                "Data":  {
                        "ind": geo_result["ind"],
                        "timestamp": geo_result["timestamp"], # may have to convert this to a string and have it converted back later?
                        "latitude": geo_result["latitude"],
                        "longitude": geo_result["longitude"],
                        "country": geo_result["country"]
                    },
                    "PartitionKey": "pintrest_test"                    
            }, default=str)

            geo_response = requests.request("PUT","https://jl1y5b7yai.execute-api.us-east-1.amazonaws.com/dev/streams/streaming-0e95b18877fd-geo/record", headers=headers, data=geo_payload)
            print(geo_payload, geo_response.json(), geo_response.status_code) 
            
            user_payload = json.dumps({
                "StreamName": "streaming-0e95b18877fd-user",
                "records": 
                    {
                        "index": user_result["ind"],
                        "first_name": user_result["first_name"],
                        "last_name": user_result["last_name"],
                        "age": user_result["age"],
                        "date_joined": user_result["date_joined"]
                    },
                    "PartitionKey": "pintrest_test"
                }, default=str)
            user_response = requests.request("PUT","https://jl1y5b7yai.execute-api.us-east-1.amazonaws.com/dev/streams/streaming-0e95b18877fd-user/record", headers=headers, data=user_payload)
            print(user_payload, user_response.json(), user_response.status_code) 

if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    


    


