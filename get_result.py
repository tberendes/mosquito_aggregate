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
    status_only = False
    if "mode" in event and event['mode'] == "status":
        status_only = True

    result_filename = "results/" + request_id + ".json"

    my_bucket = s3.Bucket(data_bucket)

    status = load_json_from_s3(my_bucket, "status/" + request_id + ".json")
    if "message" in status and status["message"] == "error":
        result_json = {"status": "waiting", "message": "Job not started yet for request_id " + request_id}
    else:
        if status["status"] == "failed":
            result_json = {"status": "failed", "message": status["type"] + ":" + status["message"]}
        elif status["status"] == "success":
            if status_only:
                result_json = {"status": "success"}
            else:
                result_file_json = load_json_from_s3(my_bucket, result_filename)
                if "message" in result_file_json and result_file_json["message"] == "error":
                    result_json = {"status": "failed", "message": status["type"] + ": cannot read result file " +
                                                                  result_filename}
                else:
                    result_json = {"status": "success", "result":result_file_json}
        else:
            result_json = {"status": status["status"], "message": status["type"] + ": " +status["message"]}

    #     found = False
    #     for result_file in my_bucket.objects.filter(Prefix="results/"):
    # #        print(result_file.key)
    #         if result_file.key == result_filename:
    #             found=True
    #     if found:
    # #        result_file_json = load_json(data_bucket, result_filename)
    #         result_file_json = load_json_from_s3(my_bucket, result_filename)
    #         result_json = {"status": "success", "result":result_file_json}
    #     else:
    #         status = load_json_from_s3(my_bucket, "status/" + request_id + ".json")
    #         result_json = {"status": status}

    return dict(statusCode='200', headers={'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                body=json.dumps(result_json), isBase64Encoded='false')


