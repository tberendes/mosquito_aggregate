import string
import json
import random

import boto3 as boto3
from  mosquito_util import load_json_from_s3

data_bucket = "mosquito-data"

s3 = boto3.resource(
    's3')

def lambda_handler(event, context):

    print("event ", event)

    if 'body' in event:
        event = json.loads(event['body'])

    request_id = event['request_id']
    result_filename = "results/"+request_id+".json"

    my_bucket = s3.Bucket(data_bucket)

    found = False
    for result_file in my_bucket.objects.filter(Prefix="results/"):
        print(result_file.key)
        if result_file.key == result_filename:
            found=True
    if found:
#        result_file_json = load_json(data_bucket, result_filename)
        result_file_json = load_json_from_s3(my_bucket, result_filename)
        result_json = {"status": "success", "result":result_file_json}
    else:
        result_json = {"status": "No results"}

    return dict(statusCode='200', headers={'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                body=json.dumps(result_json), isBase64Encoded='false')


