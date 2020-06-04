# Extract
import pymysql
from os import environ
import time
import boto3
import pandas as pd
import io

# logger = logging.getLogger(__name__) 

class Extract():

    def get_data_from_bucket(self, filename):
        # 
        s3 = boto3.client('s3')
        key = f"{filename}"
        obj = s3.get_object(Bucket= "sainos-bucket", Key= key)
        initial_df = pd.read_csv(io.BytesIO(obj['Body'].read()))
        data_list = initial_df.values.tolist()
        s3.delete_object(Bucket= "sainos-bucket", Key= key)
        # print(data_list)
        return data_list



