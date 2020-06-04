import json
import logging
from ETL.ETL import run_etl



def auto_etl(event, context):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    file_key = event['Records'][0]['s3']['object']['key']
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    logger.info('Reading {} from {}'.format(file_key, bucket_name))    
    
    run_etl(file_key)

    body = {
        "message": "Go Serverless v1.0! Your function executed successfully!",
    }

    response = {
        "statusCode": 200,
        "body": body
    }

    return response


    
if __name__ == "__main__":
    auto_etl('{"Records":[{"s3":{"object":{"key":"transactions/test.csv"}}}]}', '')