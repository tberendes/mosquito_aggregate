import string
import json
import random

import boto3 as boto3

data_bucket = "mosquito-data"

s3 = boto3.resource(
    's3')

def load_json(bucket, key):

    print("event key " + key)
    # strip off directory from key for temp file
    key_split = key.split('/')
    download_fn=key_split[len(key_split) - 1]
    file = "/tmp/" + download_fn
    s3.Bucket(bucket).download_file(key, file)

    try:
        with open(file) as f:
            jsonData = json.load(f)
        f.close()
    except IOError:
        print("Could not read file:" + file)
        jsonData = {"message": "Error reading json file"}

    return jsonData

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
        result_json = {"status": "success"}
        result_json.append(load_json(data_bucket, result_filename))
    else:
        result_json = {"status": "No results"}

    return dict(statusCode='200', headers={'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                body=json.dumps(result_json), isBase64Encoded='false')


