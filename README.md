# Pintrest Data Pipeline

## Rough notes to be made into readme later:

### Useful commands:

To run the user posting stream emulation (FROM > ~/Pintrest/pintrest_infrastructure ):

    python user_posting_emulation.py

To log into the EC2 instance (FROM > ~/Pintrest/pintrest_infrastructure ):

    ssh -i "0e95b18877fd-key-pair.pem" ec2-user@ec2-54-158-15-238.compute-1.amazonaws.com

To start the restful API (Log into AWS EC2 instance > /home/ec2-user/confluent-7.2.0/bin):

    ./kafka-rest-start /home/ec2-user/confluent-7.2.0/etc/kafka-rest/kafka-rest.properties

To run a consumer for each topic:

- 0e95b18877fd.pin

        ./kafka-console-consumer.sh --bootstrap-server b-2.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098,b-1.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098,b-3.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098 --consumer.config client.properties --group students --topic 0e95b18877fd.pin --from-beginning
- 0e95b18877fd.geo

        ./kafka-console-consumer.sh --bootstrap-server b-2.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098,b-1.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098,b-3.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098 --consumer.config client.properties --group students --topic 0e95b18877fd.geo --from-beginning
- 0e95b18877fd.user

        ./kafka-console-consumer.sh --bootstrap-server b-2.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098,b-1.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098,b-3.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098 --consumer.config client.properties --group students --topic 0e95b18877fd.user --from-beginning

### So far...

- user_posting_emulation.py

    This file runs an indefinite loop which emulates a stream of user posts on the website pintrest. It does this by drawing random lines from a large fixed database on AWS using SQL alchemy.

    All the code which connects to the AWS database is held within this class

        class AWSDBConnector:
    
    Accesses a YAML with the database credentials which is not published on git to protect the database credentials

        with open("db_creds.yaml", "r") as f:
            creds = yaml.safe_load(f)
            return creds
    
    Creates the SQLalchemy engine which enables communication with the AWS database:

        def create_db_connector(self):
            creds = self.read_db_creds()
            engine = sqlalchemy.create_engine(f"mysql+pymysql://{creds['USER']}:{creds['PASSWORD']}@{creds['HOST']}:{creds['PORT']}/{creds['DATABASE']}?charset=utf8mb4")
            return engine
    
    sets up a new class instance called new_connector

        new_connector = AWSDBConnector()
    
    This while block uses the random module and the database connector class to create and return 3 sets of data: 
    
    - pin_result

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)
        
        - **pin_string** is the query which retrieves a single"random_row" (using LIMIT 1) from the pintrest_data table
            
        - **pin_selected_row** executes this query using the initialised engine

        - The for loop then stores these as a dictionary data type in **pin_result**

    - **geo result** (Same logic / method as pin_result)
    - **user result** (Same logic / method as pin_result)
    

    This is the first example of the 3 JSON files which are sent to their respective topics. Each has its own set of keys according to layout of the data received:

            pin_payload = json.dumps({
                "records": [
                    {
                        "value":{
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
                        "category": pin_result["category"]}
                    }
                ]
            })
            headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}

    To send the data to the API:

        pin_response = requests.request("POST","https://jl1y5b7yai.execute-api.us-east-1.amazonaws.com/dev/topics/0e95b18877fd.pin", headers=headers, data=pin_payload)


- New databricks files and actions

    - Mount_s3 notebook created
        - mounts the S3 bucket where our .pin / .geo / .user topics are storing the posts as Json files
        - access keys were created for us and ssafe loaded from "dbfs:/user/hive/warehouse/authentication_credentials"
        - mounted at: "/mnt/0e95b18877fd-S3"

    - create_s3_dataframes notebook created
        - accesses each topic in the s3 bucket and reads all of the json files into a dataframe
            - df_pin
            - df_geo
            - df_user
        - Verifys that these have been uploaded correctly but calling display(df_xxx.describe()) for each
        
